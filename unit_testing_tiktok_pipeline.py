import pytest
import os
import sys
from unittest.mock import MagicMock

# Set fake env vars before importing the pipeline
os.environ["RAPIDAPI_KEY"] = "fake"
os.environ["SUPABASE_URL"] = "https://fake.supabase.co"
os.environ["SUPABASE_KEY"] = "fake"

sys.modules["supabase"] = MagicMock()

from tiktok_pipeline import (
   parse_int,
    parse_money,
    has_valid_metrics,
    pick_top_products,
    extract_price_and_currency,
    extract_sales,
    normalize_text,
    flatten_categories,
    iter_products,
    is_target,
    product_row,
    collect_text,
    to_text,
)

# to run use pytest instead of python

# flat function tests (no seperate classes) for some of the high priority functions 
# with paremtrized tuples to use at each test run instead of hardcoding vals at each fucntion

@pytest.mark.parametrize("input_val, expected", [
    # None/empty/invalid inputs default to 0
    (None, 0),
    ("", 0),
    ("-", 0),
    ("abc", 0),

    # Already an int passes through
    (41, 41),

    # String number parses normally
    ("100", 100),

    # K suffix is stripped without multiplying
    ("5K", 5),

    # Empty string edge case
    ("", 0),
])
def test_parse_int(input_val, expected):
    assert parse_int(input_val) == expected


@pytest.mark.parametrize("input_val, expected", [
    # None/empty/invalid inputs default to 0.0
    (None, 0.0),
    ("", 0.0),
    ("-", 0.0),
    ("abc", 0.0),

    # Numeric types pass through as float
    (100, 100.0),
    (9.99, 9.99),

    # Dollar sign stripped, parsed as float
    ("$49.99", 49.99),

    # Commas and dollar sign stripped
    ("$1,234.56", 1234.56),

    # Percent sign stripped, number preserved
    ("15%", 15.0),

    # K suffix multiplies by 1,000
    ("5K", 5000.0),

    # Commas + dollar sign with no decimal
    ("$2,500", 2500.0),

    # M suffix multiplies by 1,000,000
    ("2M", 2000000.0),
])
def test_parse_money(input_val, expected):
    assert parse_money(input_val) == expected


@pytest.mark.parametrize("product, expected", [
    # Empty dict - both fields missing
    ({}, False),

    # Both fields explicitly None
    ({"sale_cnt": None, "total_sale_gmv_amt": None}, False),

    # Only revenue present, sales missing
    ({"sale_cnt": None, "total_sale_gmv_amt": "500"}, False),

    # Only sales present, revenue missing
    ({"sale_cnt": "50", "total_sale_gmv_amt": None}, False),

    # Both fields are placeholder dashes
    ({"sale_cnt": "-", "total_sale_gmv_amt": "-"}, False),

    # Both fields are empty strings
    ({"sale_cnt": "", "total_sale_gmv_amt": ""}, False),

    # Both fields valid as strings
    ({"sale_cnt": "100", "total_sale_gmv_amt": "5000"}, True),

    # Both fields valid as ints
    ({"sale_cnt": 100, "total_sale_gmv_amt": 5000}, True),
])
def test_has_valid_metrics(product, expected):
    assert has_valid_metrics(product) is expected


@pytest.mark.parametrize("product, expected_price, expected_currency", [
    # Empty dict - no price info
    ({}, "", ""),

    # Explicit None values
    ({"price": None, "currency": None}, "", ""),

    # Nested dict with "value" key
    ({"price": {"value": "9.99", "currency": "USD"}}, "9.99", "USD"),

    # Nested dict with "amount" key
    ({"price": {"amount": "19.99", "currency": "EUR"}}, "19.99", "EUR"),

    # Nested dict with no recognized price key
    ({"price": {"foo": "bar"}}, "", ""),

    # Flat price string with separate currency field
    ({"price": "14.99", "currency": "USD"}, "14.99", "USD"),

    # Fallback to min_price field
    ({"min_price": "5.00"}, "5.00", ""),

    # Fallback to sale_price field
    ({"sale_price": "7.50"}, "7.50", ""),
])
def test_extract_price_and_currency(product, expected_price, expected_currency):
    price, curency = extract_price_and_currency(product)
    assert price == expected_price
    assert curency == expected_currency


@pytest.mark.parametrize("product, expected", [
    # Empty dict - no sales field found
    ({}, ""),

    # "sales" key with int value, converted to string
    ({"sales": 500}, "500"),

    # "sold_count" key with string value
    ({"sold_count": "1000"}, "1000"),

    # "order_count" key with int value
    ({"order_count": 250}, "250"),

    # None value returns empty string
    pytest.param({"sales": None}, "", id="none-value-returns-empty-string"),
])
def test_extract_sales(product, expected):
    assert extract_sales(product) == expected




@pytest.mark.parametrize("products, top_n, min_sales, use_recent, expected_count, expected_first_title", [
    # Empty list
    ([], 10, 0, False, 0, None),
    
    # Single product above threshold
    (
        [{"sale_cnt": "100", "total_sale_gmv_amt": "5000", "title": "Product A"}],
        10, 
        50, 
        False, 
        1, 
        "Product A"
    ),
    
    # Single product below threshold (filtered out)
    (
        [{"sale_cnt": "10", "total_sale_gmv_amt": "500", "title": "Product A"}],
        10, 
        50, 
        False, 
        0, 
        None
    ),
    
    # Multiple products - test sorting by sales count
    (
        [
            {"sale_cnt": "100", "total_sale_gmv_amt": "5000", "title": "Product B"},
            {"sale_cnt": "200", "total_sale_gmv_amt": "8000", "title": "Product A"},
            {"sale_cnt": "50", "total_sale_gmv_amt": "2000", "title": "Product C"},
        ],
        10,
        0,
        False,
        3,
        "Product A"  # Should be first (highest sales)
    ),
    
    # Test top_n limit
    (
        [
            {"sale_cnt": "100", "total_sale_gmv_amt": "5000", "title": f"Product {i}"}
            for i in range(20)
        ],
        5,
        0,
        False,
        5,
        "Product 0"
    ),
    
    # Test tie-breaking by revenue
    (
        [
            {"sale_cnt": "100", "total_sale_gmv_amt": "5000", "title": "Lower Revenue"},
            {"sale_cnt": "100", "total_sale_gmv_amt": "10000", "title": "Higher Revenue"},
        ],
        10,
        0,
        False,
        2,
        "Higher Revenue"  # Same sales, but higher revenue should be first
    ),
    
    # Test use_recent flag
    (
        [
            {
                "sale_cnt": "50",
                "total_sale_7d_cnt": "200",
                "total_sale_gmv_amt": "8000",
                "title": "Recent Winner"
            },
            {
                "sale_cnt": "100",
                "total_sale_7d_cnt": "100",
                "total_sale_gmv_amt": "5000",
                "title": "Total Winner"
            },
        ],
        10,
        0,
        True,  # Using recent sales
        2,
        "Recent Winner"
    ),
    
    # Products with missing/invalid metrics
    (
        [
            {"sale_cnt": "-", "total_sale_gmv_amt": "5000", "title": "Invalid Sales"},
            {"sale_cnt": "100", "total_sale_gmv_amt": "-", "title": "Invalid Revenue"},
            {"sale_cnt": "50", "total_sale_gmv_amt": "2000", "title": "Valid Product"},
        ],
        10,
        0,
        False,
        3,
        "Valid Product"
    ),
    
    # Test min_sales threshold filtering
    (
        [
            {"sale_cnt": "100", "total_sale_gmv_amt": "5000", "title": "High Sales"},
            {"sale_cnt": "30", "total_sale_gmv_amt": "1500", "title": "Low Sales"},
            {"sale_cnt": "75", "total_sale_gmv_amt": "3000", "title": "Medium Sales"},
        ],
        10,
        50,
        False,
        2,  # Only 2 products meet min_sales=50
        "High Sales"
    ),
])
def test_pick_top_products(products, top_n, min_sales, use_recent, expected_count, expected_first_title):
    result = pick_top_products(products, top_n=top_n, min_sales=min_sales, use_recent=use_recent)
    
    # Check correct number of products returned
    assert len(result) == expected_count
    
    # Check first product if we expect any results
    if expected_count > 0 and expected_first_title:
        assert result[0].get("title") == expected_first_title


@pytest.mark.parametrize("input_text, expected", [
    # Special characters replaced with spaces
    ("Kitchen & Dining", "kitchen   dining"),
    ("Health/Wellness", "health wellness"),
    ("Air-Fryer", "air fryer"),
    
    # Uppercase converted to lowercase
    ("UPPERCASE", "uppercase"),
    ("MixedCase", "mixedcase"),
    
    # None returns empty string
    (None, ""),
    
    # Empty string stays empty
    ("", ""),
    
    # Multiple special characters
    ("Mix & Match / Test-Case", "mix   match   test case"),
    
    # Only special characters
    ("&/-", "   "),
    
    # Already normalized text
    ("already normalized", "already normalized"),
])
def test_normalize_text(input_text, expected):
    assert normalize_text(input_text) == expected


@pytest.mark.parametrize("product, expected", [
    # No categories at all
    ({}, ""),
    
    # Simple string list
    ({"categories": ["Electronics", "Gadgets"]}, "Electronics; Gadgets"),
    
    # Dict with 'name' field
    ({"categories": [{"name": "Kitchen"}, {"name": "Home"}]}, "Kitchen; Home"),
    
    # Dict with 'title' field
    ({"categories": [{"title": "Sports"}]}, "Sports"),
    
    # Dict with 'category_name' field
    ({"categories": [{"category_name": "Fitness"}]}, "Fitness"),
    
    # Dict with 'label' field
    ({"categories": [{"label": "Health"}]}, "Health"),
    
    # Mixed formats (string and dict)
    ({"categories": ["Electronics", {"name": "Gadgets"}]}, "Electronics; Gadgets"),
    
    # Category field as fallback
    ({"category": "Kitchen"}, "Kitchen"),
    
    # Both categories list and category field
    ({"categories": ["Electronics"], "category": "Gadgets"}, "Electronics; Gadgets"),
    
    # Empty list
    ({"categories": []}, ""),
    
    # Invalid category types (should skip non-dict, non-string)
    ({"categories": [123, None, "Valid"]}, "Valid"),
    
    # Dict without any recognized keys
    ({"categories": [{"unknown": "value"}]}, ""),
    
    # Multiple dicts with different key types
    (
        {"categories": [{"name": "A"}, {"title": "B"}, {"category_name": "C"}]}, 
        "A; B; C"
    ),
])
def test_flatten_categories(product, expected):
    assert flatten_categories(product) == expected


@pytest.mark.parametrize("payload, expected_count", [
    # Direct list of products
    ([{"id": "1"}, {"id": "2"}], 2),
    
    # Nested under 'data' key as list
    ({"data": [{"id": "1"}, {"id": "2"}]}, 2),
    
    # Nested under data.list
    ({"data": {"list": [{"id": "1"}]}}, 1),
    
    # Nested under data.items
    ({"data": {"items": [{"id": "1"}, {"id": "2"}]}}, 2),
    
    # Nested under data.products
    ({"data": {"products": [{"id": "1"}]}}, 1),
    
    # Nested under data.result
    ({"data": {"result": [{"id": "1"}]}}, 1),
    
    # Direct 'products' key at top level
    ({"products": [{"id": "1"}]}, 1),
    
    # Direct 'items' key at top level
    ({"items": [{"id": "1"}, {"id": "2"}]}, 2),
    
    # Direct 'list' key at top level
    ({"list": [{"id": "1"}]}, 1),
    
    # Empty payload
    ({}, 0),
    ([], 0),
    
    # Non-dict items filtered out from list
    ([{"id": "1"}, "invalid", None, {"id": "2"}], 2),
    
    # Complex nested structure
    (
        {
            "data": {
                "list": [
                    {"id": "1", "name": "Product 1"},
                    {"id": "2", "name": "Product 2"}
                ]
            }
        },
        2
    ),
])
def test_iter_products(payload, expected_count):
    result = list(iter_products(payload))
    assert len(result) == expected_count
    # Verify all results are dicts
    for item in result:
        assert isinstance(item, dict)


@pytest.mark.parametrize("product, expected", [
    # Kitchen keyword match
    ({"product_name": "Air Fryer Deluxe", "categories": []}, True),
    ({"title": "Kitchen Knife Set"}, True),
    ({"name": "Cookware Set"}, True),
    
    # Health keyword match
    ({"title": "Vitamin C Supplement"}, True),
    ({"product_name": "Wellness Pack"}, True),
    ({"name": "Skincare Routine"}, True),
    
    # Fitness keyword match
    ({"name": "Yoga Mat", "category": "Sports"}, True),
    ({"title": "Dumbbell Set"}, True),
    ({"product_name": "Resistance Band"}, True),
    
    # Home keyword match
    ({"title": "Storage Organizer"}, True),
    ({"product_name": "Cleaning Supplies"}, True),
    
    # Category match
    ({"product_name": "Random Product", "categories": ["kitchen dining"]}, True),
    ({"title": "Item", "category": "fitness"}, True),
    
    # No match - random product
    ({"product_name": "Random Toy", "category": "Toys"}, False),
    ({"title": "Book", "category": "Books"}, False),
    
    # Case insensitive matching
    ({"product_name": "KITCHEN KNIFE"}, True),
    ({"title": "Health Supplement"}, True),
    
    # Special characters normalized
    ({"product_name": "Kitchen & Dining Set"}, True),
    ({"title": "Health/Wellness Product"}, True),
    ({"name": "Air-Fryer"}, True),
    
    # Empty product
    ({}, False),
    
    # Product with no name fields
    ({"price": "10.99", "sales": "100"}, False),
    
    # Partial keyword match in longer string
    ({"product_name": "Premium Kitchenware Collection"}, True),
    ({"title": "Professional Fitness Equipment"}, True),
])
def test_is_target(product, expected):
    assert is_target(product) == expected


@pytest.mark.parametrize("input_val, expected", [
    # None returns empty string
    (None, ""),
    
    # Empty string stays empty
    ("", ""),
    
    # Regular string converted to string
    ("test", "test"),
    
    # Integer converted to string
    (123, "123"),
    
    # Float converted to string
    (45.67, "45.67"),
    
    # Boolean converted to string
    (True, "True"),
    (False, "False"),
    
    # Zero stays as string "0"
    (0, "0"),
])
def test_to_text(input_val, expected):
    assert to_text(input_val) == expected


@pytest.mark.parametrize("product, expected_product_name, expected_categories", [
    # Complete product data
    (
        {
            "product_name": "Test Product",
            "categories": ["Kitchen", "Home"]
        },
        "test product",
        "kitchen; home"
    ),
    
    # Using 'title' instead of 'product_name'
    (
        {
            "title": "Another Product",
            "category": "Fitness"
        },
        "another product",
        "fitness"
    ),
    
    # Using 'name' field
    (
        {
            "name": "Third Product",
            "categories": ["Health"]
        },
        "third product",
        "health"
    ),
    
    # Empty product
    ({}, "", ""),
    
    # Only name, no categories
    ({"product_name": "Solo Product"}, "solo product", ""),
    
    # Special characters normalized
    (
        {
            "product_name": "Kitchen & Dining",
            "categories": ["Home/Garden"]
        },
        "kitchen   dining",
        "home/garden"
    ),
])
def test_collect_text(product, expected_product_name, expected_categories):
    result = collect_text(product)
    # Result should contain the normalized product name
    assert expected_product_name in result
    # If there are categories, they should appear in the result
    if expected_categories:
        # Categories are also normalized, so check if components are present
        for cat_part in expected_categories.split("; "):
            assert cat_part in result or normalize_text(cat_part) in result


def test_product_row_complete_data():
    """Test product_row with complete data"""
    product = {
        "product_id": "123",
        "product_name": "Test Product",
        "price": "29.99",
        "currency": "USD",
        "sales": "1000",
        "categories": ["Kitchen", "Home"],
        "shop_info": {"name": "Test Shop", "shop_id": "shop_456"},
        "product_url": "https://example.com/product",
        "cover": "https://example.com/image.jpg"
    }
    
    result = product_row(product)
    
    assert result["product_id"] == "123"
    assert result["product_name"] == "Test Product"
    assert result["price"] == "29.99"
    assert result["currency"] == "USD"
    assert result["sales"] == "1000"
    assert result["categories"] == "Kitchen; Home"
    assert result["shop_name"] == "Test Shop"
    assert result["shop_id"] == "shop_456"
    assert result["product_url"] == "https://example.com/product"
    assert result["image_url"] == "https://example.com/image.jpg"


def test_product_row_minimal_data():
    """Test product_row with minimal/missing data"""
    product = {"id": "123"}  # Only ID present
    result = product_row(product)
    
    assert result["product_id"] == "123"
    assert result["product_name"] == ""
    assert result["price"] == ""
    assert result["currency"] == ""
    assert result["sales"] == ""
    assert result["categories"] == ""
    assert result["shop_name"] == ""
    assert result["shop_id"] == ""
    assert result["product_url"] == ""
    assert result["image_url"] == ""


def test_product_row_alternative_fields():
    """Test product_row uses alternative field names"""
    product = {
        "id": "456",  # Uses 'id' instead of 'product_id'
        "title": "Alt Product",  # Uses 'title' instead of 'product_name'
        "min_price": "15.00",  # Uses 'min_price' instead of 'price'
        "sold_count": "500",  # Uses 'sold_count' instead of 'sales'
        "detail_url": "https://example.com/detail",  # Alternative URL field
        "image": "https://example.com/pic.jpg"  # Alternative image field
    }
    
    result = product_row(product)
    
    assert result["product_id"] == "456"
    assert result["product_name"] == "Alt Product"
    assert result["price"] == "15.00"
    assert result["sales"] == "500"
    assert result["product_url"] == "https://example.com/detail"
    assert result["image_url"] == "https://example.com/pic.jpg"


def test_product_row_shop_info_handling():
    """Test product_row handles shop_info correctly"""
    # Valid shop_info dict
    product_with_shop = {
        "id": "1",
        "shop_info": {"name": "Shop A", "shop_id": "shop_1"}
    }
    result = product_row(product_with_shop)
    assert result["shop_name"] == "Shop A"
    assert result["shop_id"] == "shop_1"
    
    # shop_info is not a dict
    product_no_shop = {
        "id": "2",
        "shop_info": "invalid"
    }
    result = product_row(product_no_shop)
    assert result["shop_name"] == ""
    assert result["shop_id"] == ""
    
    # Missing shop_info
    product_missing = {"id": "3"}
    result = product_row(product_missing)
    assert result["shop_name"] == ""
    assert result["shop_id"] == ""


def test_product_row_preserves_structure():
    """Test that product_row returns all expected CSV fields"""
    product = {"product_id": "test"}
    result = product_row(product)
    
    # Verify all CSV_FIELDS are present
    expected_fields = [
        "product_id", "product_name", "price", "currency", "sales",
        "category", "categories", "shop_name", "shop_id",
        "product_url", "image_url"
    ]
    
    for field in expected_fields:
        assert field in result

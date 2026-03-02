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
)


from tiktok_pipeline import (
    parse_int,
    parse_money,
    has_valid_metrics,
    extract_price_and_currency,
    extract_sales,
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

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
    (None, 0),
    ("", 0),
    ("-", 0),
    ("abc", 0),
    (41, 41),
    ("100", 100),
    ("5k", 5),
])
def test_parse_int(input_val, expected):
    assert parse_int(input_val) == expected


@pytest.mark.parametrize("input_val, expected", [
    (None, 0.0),
    ("", 0.0),
    ("-", 0.0),
    ("abc", 0.0),
    (100, 100.0),
    (9.99, 9.99),
    ("$49.99", 49.99),
    ("$1,234.56", 1234.56),
    ("15%", 15.0),
    ("5K", 5000.0),
    (" $2,500 ", 2500.0),
])
def test_parse_money(input_val, expected):
    assert parse_money(input_val) == expected


@pytest.mark.parametrize("product, expected", [
    ({}, False),
    ({"sale_cnt": None, "total_sale_gmv_amt": None}, False),
    ({"sale_cnt": None, "total_sale_gmv_amt": "500"}, False),
    ({"sale_cnt": "50", "total_sale_gmv_amt": None}, False),
    ({"sale_cnt": "-", "total_sale_gmv_amt": "-"}, False),
    ({"sale_cnt": "", "total_sale_gmv_amt": ""}, False),
    ({"sale_cnt": "100", "total_sale_gmv_amt": "5000"}, True),
    ({"sale_cnt": 100, "total_sale_gmv_amt": 5000}, True),
])
def test_has_valid_metrics(product, expected):
    assert has_valid_metrics(product) is expected


@pytest.mark.parametrize("product, expected_price, expected_currency", [
    ({}, "", ""),
    ({"price": None, "currency": None}, "", ""),
    ({"price": {"value": "9.99", "currency": "USD"}}, "9.99", "USD"),
    ({"price": {"amount": "19.99", "currency": "EUR"}}, "19.99", "EUR"),
    ({"price": {"foo": "bar"}}, "", ""),
    ({"price": "14.99", "currency": "USD"}, "14.99", "USD"),
    ({"min_price": "5.00"}, "5.00", ""),
    ({"sale_price": "7.50"}, "7.50", ""),
])
def test_extract_price_and_currency(product, expected_price, expected_currency):
    price, curency = extract_price_and_currency(product)
    assert price == expected_price
    assert curency == expected_currency


@pytest.mark.parametrize("product, expected", [
    ({}, ""),
    ({"sales": 500}, "500"),
    ({"sold_count": "1000"}, "1000"),
    ({"order_count": 250}, "250"),
    pytest.param({"sales": None}, "", id="none-value-returns-empty-string"),
])
def test_extract_sales(product, expected):
    assert extract_sales(product) == expected
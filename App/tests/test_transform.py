import pytest
import pandas as pd
from App.etl.transform import (
    normalize_customers,
    normalize_products,
    normalize_sales,
    transform_dates_sales,
    create_sales_full,
    transformed_data
)


@pytest.fixture
def customers_df():
    return pd.DataFrame([
        {"customer_id": 1, "first_name": "John", "last_name": "Doe", "email": "john.doe@example.com",
         "country": "usa", "birth_date": "1980-01-01", "gender": "M", "loyalty_points": 100},
        {"customer_id": 2, "first_name": None, "last_name": "Smith", "email": "invalid_email",
         "country": None, "birth_date": "3000-01-01", "gender": None, "loyalty_points": -50}
    ])

@pytest.fixture
def products_df():
    return pd.DataFrame([
        {"product_id": 1, "product_name": "Laptop", "category": "Electronics",
         "price": 1000.50, "stock_quantity": 10, "supplier": "TechCo"},
        {"product_id": 2, "product_name": None, "category": None,
         "price": None, "stock_quantity": None, "supplier": None}
    ])

@pytest.fixture
def sales_df():
    return pd.DataFrame([
        {"sale_id": 1, "customer_id": 1, "product_id": 1, "quantity": 2,
         "sale_date": "2025-01-01", "payment_method": "Credit Card", "store_location": "NY"},
        {"sale_id": 2, "customer_id": 2, "product_id": 2, "quantity": -5,
         "sale_date": "1800-01-01", "payment_method": "Unknown", "store_location": "LA"}
    ])



def test_normalize_customers(customers_df):
    """Test normalization of customer DataFrame, email validation, and loyalty_points correction."""

    df = normalize_customers(customers_df)
    assert "email_clean" in df.columns
    assert "is_email_valid" in df.columns
    assert df["loyalty_points"].min() >= 0


def test_normalize_products(products_df):
    """Test normalization of product DataFrame, price and stock quantity adjustments."""

    df = normalize_products(products_df)
    assert "price" in df.columns
    assert "stock_quantity" in df.columns
    assert df["price"].min() >= 0
    assert df["stock_quantity"].min() >= 0


def test_normalize_sales(sales_df):
    """Test normalization of sales DataFrame including validity flags."""

    df = normalize_sales(sales_df)
    assert "sale_is_valid" in df.columns
    assert df["sale_is_valid"].dtype == bool


def test_transform_dates_sales(sales_df):
    """Test transformation of sale_date into separate year, month, day columns."""

    df = normalize_sales(sales_df)
    df = transform_dates_sales(df)
    for col in ["sale_year", "sale_month", "sale_day"]:
        assert col in df.columns


def test_create_sales_full(customers_df, products_df, sales_df):
    """Test merging sales with customers and products to compute total_value_sales."""

    customers = normalize_customers(customers_df)
    products = normalize_products(products_df)
    sales = transform_dates_sales(normalize_sales(sales_df))
    df_full = create_sales_full(sales, products, customers)
    
    assert "total_value_sales" in df_full.columns
    assert df_full["total_value_sales"].dtype == float
    
    # All total values should be >= 0
    assert (df_full["total_value_sales"] >= 0).all()


@pytest.fixture
def mock_extract(monkeypatch):
    """Patch extract_and_validate to provide dummy data for testing the full transform pipeline."""

    dummy_data = {
        "customers_df": pd.DataFrame([{
            "customer_id": 1, "first_name": "John", "last_name": "Doe",
            "email": "john@example.com", "country": "US",
            "birth_date": "1980-01-01", "gender": "M", "loyalty_points": 100
        }]),
        "products_df": pd.DataFrame([{
            "product_id": 1, "product_name": "Laptop", "category": "Electronics",
            "price": 1000, "stock_quantity": 10, "supplier": "TechCo"
        }]),
        "sales_df": pd.DataFrame([{
            "sale_id": 1, "customer_id": 1, "product_id": 1, "quantity": 2,
            "sale_date": "2025-01-01", "payment_method": "Credit Card",
            "store_location": "NY"
        }])
    }
    monkeypatch.setattr("App.etl.transform.extract_and_validate", lambda: dummy_data)

def test_transformed_data_pipeline(mock_extract):
    """Test full ETL transformation pipeline from normalized inputs to sales_full_df output."""

    result = transformed_data()

    expected_keys = ["customers_df", "products_df", "sales_df", "sales_full_df"]
    
    # Check that all expected keys exist
    for key in expected_keys:
        assert key in result

    # Check that DataFrames are not empty
    for df in result.values():
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    # Verify total_value_sales column exists and all values are >= 0
    sales_full = result["sales_full_df"]
    assert "total_value_sales" in sales_full.columns
    assert (sales_full["total_value_sales"] >= 0).all()

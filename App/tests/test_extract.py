import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import patch
from App.etl.extract import (
    read_csv_safe,
    validate_dataframes,
    check_dtypes,
    extract_and_validate
)


@pytest.fixture
def sample_customers():
    return pd.DataFrame({
        "customer_id": [1, 2],
        "first_name": ["Alice", "Bob"],
        "last_name": ["Smith", "Jones"],
        "email": ["a@mail.com", "b@mail.com"],
        "country": ["US", "UK"],
        "birth_date": pd.to_datetime(["1990-01-01", "1985-05-05"]),
        "gender": ["F", "M"],
        "loyalty_points": [10, 20]
    })

@pytest.fixture
def sample_products():
    return pd.DataFrame({
        "product_id": [101, 102],
        "product_name": ["Widget", "Gadget"],
        "category": ["Tools", "Tech"],
        "price": [19.99, 29.99],
        "stock_quantity": [100, 200],
        "supplier": ["Supplier1", "Supplier2"]
    })

@pytest.fixture
def sample_sales():
    return pd.DataFrame({
        "sale_id": [1001, 1002],
        "customer_id": [1, 2],
        "product_id": [101, 102],
        "quantity": [1, 2],
        "sale_date": pd.to_datetime(["2025-11-10", "2025-11-11"]),
        "payment_method": ["card", "cash"],
        "store_location": ["NY", "LA"]
    })


def test_read_csv_safe_valid(tmp_path):
    """Test reading a valid CSV file returns correct DataFrame."""

    file_path = tmp_path / "valid.csv"
    df_expected = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    df_expected.to_csv(file_path, index=False)

    df_result = read_csv_safe(file_path, "valid.csv")

    assert df_result is not None
    assert isinstance(df_result, pd.DataFrame)
    assert len(df_result) == 2
    assert list(df_result.columns) == ["id", "name"]


def test_read_csv_safe_empty_file(tmp_path, caplog):
    """Test reading an empty CSV logs warning and returns DataFrame or None."""

    file_path = tmp_path / "empty.csv"
    file_path.write_text("id,name\n")

    with caplog.at_level("WARNING"):
        result = read_csv_safe(file_path, name="EmptyTest")

    assert result is None or isinstance(result, pd.DataFrame)
    assert any("Read with success but is Empty" in message for message in caplog.messages)


def test_read_csv_safe_file_not_found(caplog):
    """Test reading a non-existent file logs error and returns None."""

    with caplog.at_level("ERROR"):
        result = read_csv_safe("nonexistent.csv", name="NotFound")

    assert result is None
    assert any("File Not Found" in message for message in caplog.messages)


def test_read_csv_safe_generic_exception(monkeypatch):
    """Test that a generic exception in pd.read_csv is handled and returns None."""

    def fake_read_csv(*args, **kwargs):
        raise ValueError("Generic failure")

    monkeypatch.setattr(pd, "read_csv", fake_read_csv)
    result = read_csv_safe(Path("fake.csv"), "fake.csv")
    assert result is None


def test_check_dtypes_missing_column(caplog):
    """Test missing column logs error and DataFrame remains unchanged."""

    df = pd.DataFrame({"a": [1, 2]})
    expected = {"a": "int64", "b": "int64"}

    caplog.set_level("ERROR", logger="App.etl.extract")
    result = check_dtypes(df, expected, "test_df")

    assert "b" not in result.columns
    assert any("Column 'b' not found in test_df." in msg for msg in caplog.messages)


def test_check_dtypes_numeric_conversion():
    """Test numeric conversion from strings with invalid entries produces NaN."""

    df = pd.DataFrame({"a": ["1", "2", "x"]})
    expected = {"a": "int64"}

    result = check_dtypes(df, expected, "test_df")
    
    assert pd.api.types.is_numeric_dtype(result["a"])
    assert pd.isna(result["a"].iloc[2])


def test_check_dtypes_float64_conversion():
    """Test float64 conversion from strings with invalid entries produces NaN."""

    df = pd.DataFrame({"price": ["10.5", "20", "abc", None]})
    expected_types = {"price": "float64"}

    df_converted = check_dtypes(df, expected_types, "test_float")
    assert df_converted["price"].dtype == "float64"
    assert df_converted["price"].iloc[0] == 10.5
    assert df_converted["price"].iloc[1] == 20.0
    assert pd.isna(df_converted["price"].iloc[2])
    assert pd.isna(df_converted["price"].iloc[3])


def test_check_dtypes_datetime_conversion():
    """Test datetime conversion with invalid values produces NaT."""

    df = pd.DataFrame({"date": ["2023-01-01", "invalid"]})
    expected = {"date": "datetime64[ns]"}

    result = check_dtypes(df, expected, "test_df")
    assert pd.api.types.is_datetime64_any_dtype(result["date"])
    assert pd.isna(result["date"].iloc[1])


def test_check_dtypes_str_conversion():
    """Test conversion to string type."""

    df = pd.DataFrame({"a": [1, 2]})
    expected = {"a": "object"}

    result = check_dtypes(df, expected, "test_df")
    assert result["a"].dtype == "object"
    assert all(isinstance(x, str) for x in result["a"])


def test_validate_dataframes_valid(sample_customers, sample_products, sample_sales):
    """Test that validate_dataframes converts types correctly and returns all DataFrames."""

    result = validate_dataframes(sample_customers, sample_products, sample_sales)

    assert set(result.keys()) == {"customers_df", "products_df", "sales_df"}
    assert pd.api.types.is_integer_dtype(result["customers_df"]["customer_id"])
    assert pd.api.types.is_float_dtype(result["products_df"]["price"])
    assert pd.api.types.is_datetime64_any_dtype(result["sales_df"]["sale_date"])


def test_validate_dataframes_type_correction():
    """Test validate_dataframes handles string numeric and datetime conversions with NaN."""

    customers = pd.DataFrame({
        "customer_id": ["1","2"],
        "birth_date": ["1990-01-01","notadate"],
        "loyalty_points": ["10","abc"],
        "first_name": ["Alice","Bob"],
        "last_name": ["Smith","Jones"],
        "email": ["a@mail.com","b@mail.com"],
        "country": ["US","UK"],
        "gender": ["F","M"]
    })

    result = validate_dataframes(customers, None, None)

    assert pd.api.types.is_numeric_dtype(result["customers_df"]["customer_id"])
    assert pd.api.types.is_numeric_dtype(result["customers_df"]["loyalty_points"])
    assert pd.isna(result["customers_df"]["loyalty_points"].iloc[1])
    assert pd.api.types.is_datetime64_any_dtype(result["customers_df"]["birth_date"])
    assert pd.isna(result["customers_df"]["birth_date"].iloc[1])


def test_validate_dataframes_none_input():
    """Test validate_dataframes returns empty dict when all inputs are None."""

    result = validate_dataframes(None, None, None)
    assert result == {}


def test_validate_dataframes_missing_columns(caplog):
    """Test validate_dataframes logs warnings when expected columns are missing."""

    customers = pd.DataFrame({
        "customer_id": [1, 2],
        "first_name": ["Alice", "Bob"]
    })

    with caplog.at_level("WARNING"):
        result = validate_dataframes(customers, None, None)

    warnings = [record.message for record in caplog.records]
    assert any("Column" in w and "not found" in w for w in warnings)


def test_extract_and_validate_valid_csvs(sample_customers, sample_products, sample_sales):
    """Test extract_and_validate integrates CSV reading and validation correctly."""

    with patch("App.etl.extract.read_csv_safe", side_effect=[sample_customers, sample_products, sample_sales]):
        result = extract_and_validate()

    assert set(result.keys()) == {"customers_df", "products_df", "sales_df"}
    assert pd.api.types.is_integer_dtype(result["customers_df"]["customer_id"])
    assert pd.api.types.is_float_dtype(result["products_df"]["price"])
    assert pd.api.types.is_datetime64_any_dtype(result["sales_df"]["sale_date"])


def test_extract_and_validate_some_none(sample_customers):
    """Test extract_and_validate handles None CSVs gracefully."""
    with patch("App.etl.extract.read_csv_safe", side_effect=[sample_customers, None, None]):
        result = extract_and_validate()

    assert "customers_df" in result
    assert "products_df" not in result
    assert "sales_df" not in result


def test_extract_and_validate_logs(caplog):
    """Test extract_and_validate logs warnings when columns are missing."""

    customers = pd.DataFrame({"customer_id": [1]})
    with patch("App.etl.extract.read_csv_safe", side_effect=[customers, None, None]), caplog.at_level("WARNING"):
        result = extract_and_validate()

    warnings = [record.message for record in caplog.records]
    assert any("Column" in w and "not found" in w for w in warnings)

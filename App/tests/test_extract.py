import pandas as pd
import App.etl.extract as extract

from pathlib import Path
from unittest.mock import patch
from App.etl.extract import read_csv_safe, validate_dataframes, check_dtypes, extract_and_validate



def test_read_csv_safe_valid(tmp_path):
    """Verify that the valid CSV file is read correctly."""

    file_path = tmp_path / "valid.csv"
    df_expected = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    df_expected.to_csv(file_path, index=False)

    df_result = read_csv_safe(file_path, "valid.csv")

    assert df_result is not None
    assert isinstance(df_result, pd.DataFrame)
    assert len(df_result) == 2
    assert list(df_result.columns) == ["id", "name"]


def test_read_csv_safe_dataframe_empty(tmp_path, caplog):
    """Verify that the valid CSV file is read correctly but dataframe is empty."""

    file_path = tmp_path / "empty.csv"
    file_path.write_text("id,name\n")
    
    with caplog.at_level("WARNING"):
        result = extract.read_csv_safe(file_path, name="EmptyTest")
    
    assert result is None or isinstance(result, pd.DataFrame)
    assert any("Read with success but is Empty" in message for message in caplog.messages)


def test_read_csv_safe_generic_exception(monkeypatch, tmp_path):
    """Force a generic exception to cover except Exception."""
    def fake_read_csv(*args, **kwargs):
        raise ValueError("Generic failure")

    monkeypatch.setattr(pd, "read_csv", fake_read_csv)
    result = read_csv_safe(Path("fake.csv"), "fake.csv")
    assert result is None


def test_read_csv_not_found(tmp_path, caplog):
    """Verify that csv file not found"""

    file_path = tmp_path / "x.csv"
    file_path.write_text("id,name\n")
    
    with caplog.at_level("ERROR"):
        result = extract.read_csv_safe("test", name="NotFound")
    
    assert result is None or isinstance(result, pd.DataFrame)
    assert any("File Not Found:" in message for message in caplog.messages)


def test_read_csv_safe_file_is_empty(tmp_path, caplog):
    """Verify that the valid CSV file is read correctly but file is empty."""

    file_path = tmp_path / "empty.csv"
    file_path.write_text(" ")
    
    with caplog.at_level("ERROR"):
        result = extract.read_csv_safe(file_path, name="empty")
    
    assert result is None or isinstance(result, pd.DataFrame)
    assert any("File Empty:" in message for message in caplog.messages)
    

def test_read_csv_safe_error_parser(tmp_path, caplog):
    """Verify that the valid CSV file is read correctly but file is error to parse."""

    file_path = tmp_path / "parser.csv"
    file_path.write_text("id,name\n1,Alice\n2,Bob,ExtraColumn")
    df_result = read_csv_safe(file_path, "corrupted.csv")

    with caplog.at_level("ERROR"):
        result = extract.read_csv_safe(file_path, name="parser")

    assert df_result is None or isinstance(result, pd.DataFrame)
    assert any("Error in Parser" in message for message in caplog.messages)


def test_check_dtypes_missing_column(caplog):
    
    df = pd.DataFrame({"a": [1, 2]})
    expected = {"a": "int64", "b": "int64"}

    caplog.set_level("ERROR", logger="App.etl.extract")
    result = extract.check_dtypes(df, expected, "test_df")

    assert "b" not in result.columns
    assert any("Column 'b' not found in test_df." in msg for msg in caplog.messages)


def test_check_dtypes_correct_type():

    df = pd.DataFrame({"a": [1, 2]})
    expected = {"a": "int64"}

    result = check_dtypes(df, expected, "test_df")
    assert result["a"].dtype == "int64"


def test_check_dtypes_convert_numeric():

    df = pd.DataFrame({"a": ["1", "2", "x"]})
    expected = {"a": "int64"}

    result = check_dtypes(df, expected, "test_df")
    
    assert pd.api.types.is_numeric_dtype(result["a"])
    assert pd.isna(result["a"].iloc[2])

def test_check_dtypes_exception():
    

    df = pd.DataFrame({"a": ["1", "2", "x"]})
    expected = {"a": "int64"}

    result = check_dtypes(df, expected, "test_df")
    
    assert pd.api.types.is_numeric_dtype(result["a"])
    assert pd.isna(result["a"].iloc[2])


def test_check_dtypes_convert_datetime():

    df = pd.DataFrame({"date": ["2023-01-01", "invalid"]})
    expected = {"date": "datetime64[ns]"}

    result = check_dtypes(df, expected, "test_df")
    assert pd.api.types.is_datetime64_any_dtype(result["date"])
    assert pd.isna(result["date"].iloc[1])


def test_check_dtypes_convert_str():

    df = pd.DataFrame({"a": [1, 2]})
    expected = {"a": "object"}

    result = check_dtypes(df, expected, "test_df")
    assert result["a"].dtype == "object"
    assert all(isinstance(x, str) for x in result["a"])


def test_validate_dataframes_valid():

    customers = pd.DataFrame({
        "customer_id": [1,2],
        "first_name": ["Alice", "Bob"],
        "last_name": ["Smith", "Jones"],
        "email": ["a@mail.com","b@mail.com"],
        "country": ["US","UK"],
        "birth_date": pd.to_datetime(["1990-01-01","1985-05-05"]),
        "gender": ["F","M"],
        "loyalty_points": [10,20]
    })

    products = pd.DataFrame({
        "product_id": [101,102],
        "product_name": ["Widget","Gadget"],
        "category": ["Tools","Tech"],
        "price": [19.99, 29.99],
        "stock_quantity": [100,200],
        "supplier": ["Supplier1","Supplier2"]
    })

    sales = pd.DataFrame({
        "sale_id": [1001,1002],
        "customer_id": [1,2],
        "product_id": [101,102],
        "quantity": [1,2],
        "sale_date": pd.to_datetime(["2025-11-10","2025-11-11"]),
        "payment_method": ["card","cash"],
        "store_location": ["NY","LA"]
    })

    result = validate_dataframes(customers, products, sales)

    assert set(result.keys()) == {"customers_df","products_df","sales_df"}
    assert pd.api.types.is_integer_dtype(result["customers_df"]["customer_id"])
    assert pd.api.types.is_float_dtype(result["products_df"]["price"])
    assert pd.api.types.is_datetime64_any_dtype(result["sales_df"]["sale_date"])


def test_validate_dataframes_type_correction():

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
    result = validate_dataframes(None, None, None)
    assert result == {}


def test_validate_dataframes_missing_columns(caplog):

    customers = pd.DataFrame({
        "customer_id": [1,2],
        "first_name": ["Alice","Bob"]
        # outras colunas ausentes
    })

    with caplog.at_level("WARNING"):
        result = validate_dataframes(customers, None, None)

    warnings = [record.message for record in caplog.records]
    assert any("Column" in w and "not found" in w for w in warnings)


def test_extract_and_validate_valid_csvs():
    customers = pd.DataFrame({"customer_id":[1], "first_name":["Alice"], "last_name":["Smith"],
                              "email":["a@mail.com"], "country":["US"], "birth_date":pd.to_datetime(["1990-01-01"]),
                              "gender":["F"], "loyalty_points":[10]})
    products = pd.DataFrame({"product_id":[101], "product_name":["Widget"], "category":["Tools"],
                             "price":[19.99], "stock_quantity":[100], "supplier":["Supplier1"]})
    sales = pd.DataFrame({"sale_id":[1001], "customer_id":[1], "product_id":[101],
                          "quantity":[1], "sale_date":pd.to_datetime(["2025-11-10"]),
                          "payment_method":["card"], "store_location":["NY"]})

    with patch("App.etl.extract.read_csv_safe", side_effect=[customers, products, sales]):
        result = extract_and_validate()
    
    assert set(result.keys()) == {"customers_df","products_df","sales_df"}
    assert pd.api.types.is_integer_dtype(result["customers_df"]["customer_id"])
    assert pd.api.types.is_float_dtype(result["products_df"]["price"])
    assert pd.api.types.is_datetime64_any_dtype(result["sales_df"]["sale_date"])


def test_extract_and_validate_some_none():
    customers = pd.DataFrame({"customer_id":[1], "first_name":["Alice"], "last_name":["Smith"],
                              "email":["a@mail.com"], "country":["US"], "birth_date":pd.to_datetime(["1990-01-01"]),
                              "gender":["F"], "loyalty_points":[10]})
    with patch("App.etl.extract.read_csv_safe", side_effect=[customers, None, None]):
        result = extract_and_validate()
    
    assert "customers_df" in result
    assert "products_df" not in result
    assert "sales_df" not in result


def test_extract_and_validate_logs(caplog):
    customers = pd.DataFrame({"customer_id":[1]})  # colunas faltantes
    with patch("App.etl.extract.read_csv_safe", side_effect=[customers, None, None]), caplog.at_level("WARNING"):
        result = extract_and_validate()
    
    warnings = [record.message for record in caplog.records]
    assert any("Column" in w and "not found" in w for w in warnings)
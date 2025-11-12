import pytest
import pandas as pd
from unittest.mock import patch
from App.etl import load


def make_mock_df():
    """Create a mock DataFrame with a patched to_sql method to count calls."""

    df = pd.DataFrame({"col": [1]})
    df.to_sql_called = 0

    original_to_sql = df.to_sql

    # Fake to_sql to count calls
    def fake_to_sql(*args, **kwargs):
        df.to_sql_called += 1
        return None

    df.to_sql = fake_to_sql
    return df


@patch("App.etl.load.get_engine")
@patch("App.etl.load.transformed_data")
def test_load_data(mock_transformed_data, mock_get_engine):
    """
    Test that load_data() calls the to_sql method exactly once for each table.
    Ensures that all four tables (customers, products, sales, sales_full) are attempted to be loaded.
    """

    mock_engine = "engine"
    mock_get_engine.return_value = mock_engine

    # Create DataFrames with patched to_sql
    customers = make_mock_df()
    products = make_mock_df()
    sales = make_mock_df()
    sales_full = make_mock_df()

    mock_transformed_data.return_value = {
        "customers_df": customers,
        "products_df": products,
        "sales_df": sales,
        "sales_full_df": sales_full,
    }

    load.load_data()

    # Check that to_sql was called once for each table
    for df, table in zip([customers, products, sales, sales_full],
                         ["customers", "products", "sales", "sales_full"]):
        assert df.to_sql_called == 1, f"to_sql should be called once for table {table}"


@patch("App.etl.load.get_engine")
def test_create_tables(mock_get_engine):
    """
    Test that create_tables() calls Base.metadata.create_all() with the database engine.
    Ensures that the tables are created if they do not exist.
    """

    mock_engine = "engine"
    mock_get_engine.return_value = mock_engine

    with patch.object(load.Base.metadata, "create_all") as mock_create_all:
        load.create_tables()
        mock_create_all.assert_called_once_with(mock_engine)


@patch("App.etl.load.create_engine")
@patch("App.etl.load.database_exists")
@patch("App.etl.load.create_database")
@patch("App.etl.load.get_database_url")
def test_get_engine_database_not_exists(mock_get_url, mock_create_db, mock_db_exists, mock_create_engine):
    """
    Test get_engine() when the database does not exist.
    Ensures that create_database() is called to create the missing database.
    """

    mock_get_url.return_value = "postgresql://u:p@localhost:5432/db"

    mock_engine = mock_create_engine.return_value
    mock_engine.url.database = "db"
    mock_db_exists.return_value = False

    engine = load.get_engine()

    mock_create_db.assert_called_once_with(mock_engine.url)
    assert engine == mock_engine


@patch("App.etl.load.create_engine")
@patch("App.etl.load.database_exists")
@patch("App.etl.load.create_database")
@patch("App.etl.load.get_database_url")
def test_get_engine_database_exists(mock_get_url, mock_create_db, mock_db_exists, mock_create_engine):
    """
    Test get_engine() when the database already exists.
    Ensures that create_database() is not called.
    """

    mock_get_url.return_value = "postgresql://u:p@localhost:5432/db"

    mock_engine = mock_create_engine.return_value
    mock_db_exists.return_value = True

    engine = load.get_engine()

    mock_create_db.assert_not_called()
    assert engine == mock_engine


def test_get_database_url_success(monkeypatch):
    """
    Test that get_database_url() returns the correct PostgreSQL URL
    when all required environment variables are set.
    """

    monkeypatch.setenv("POSTGRES_USER", "user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "pass")
    monkeypatch.setenv("POSTGRES_DB", "db")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")

    url = load.get_database_url()
    assert url == "postgresql://user:pass@localhost:5432/db"


def test_get_database_url_missing_env(monkeypatch):
    """
    Test that get_database_url() raises an EnvironmentError
    when one or more environment variables are missing.
    """
    
    # Remove environment variables
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("POSTGRES_DB", raising=False)
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.delenv("POSTGRES_PORT", raising=False)

    with pytest.raises(EnvironmentError) as exc:
        load.get_database_url()

    assert "❌ Missing PostgreSQL environment variables" in str(exc.value)

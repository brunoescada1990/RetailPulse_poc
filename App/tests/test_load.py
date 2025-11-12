import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from App.etl import load



@patch("App.etl.load.get_engine")
@patch("App.etl.load.transformed_data")
def test_load_data(mock_transformed_data, mock_get_engine):
    """Testa se load_data() chama to_sql 4 vezes com as tabelas corretas."""

    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    
    mock_df = MagicMock(spec=pd.DataFrame)
    mock_transformed_data.return_value = {
        "customers_df": mock_df,
        "products_df": mock_df,
        "sales_df": mock_df,
        "sales_full_df": mock_df,
    }

    from App.etl import load
    load.load_data()

    assert mock_df.to_sql.call_count == 4, "to_sql deve ser chamado 4 vezes (uma por tabela)"

    expected_calls = ["customers", "products", "sales", "sales_full"]
    actual_calls = [call.args[0] for call in mock_df.to_sql.call_args_list]
    assert actual_calls == expected_calls, f"Tabelas esperadas: {expected_calls}, mas foram chamadas: {actual_calls}"


@patch("App.etl.load.get_engine")  
def test_create_tables(mock_get_engine):
    """Testa se create_tables() chama Base.metadata.create_all()."""

    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    with patch.object(load.Base.metadata, "create_all") as mock_create_all:
        load.create_tables()
        mock_create_all.assert_called_once_with(mock_engine)



@patch("App.etl.load.create_engine")
@patch("App.etl.load.database_exists")
@patch("App.etl.load.create_database")
@patch("App.etl.load.get_database_url")
def test_get_engine_database_not_exists(mock_get_url, mock_create_db, mock_db_exists, mock_create_engine):
    mock_get_url.return_value = "postgresql://u:p@localhost:5432/db"
    
    mock_engine = MagicMock()
    mock_engine.url.database = "db"
    mock_create_engine.return_value = mock_engine
    mock_db_exists.return_value = False

    engine = load.get_engine()

    mock_create_db.assert_called_once_with(mock_engine.url)
    assert engine == mock_engine

@patch("App.etl.load.create_engine")
@patch("App.etl.load.database_exists")
@patch("App.etl.load.create_database")
@patch("App.etl.load.get_database_url")
def test_get_engine_database_exists(mock_get_url, mock_create_db, mock_db_exists, mock_create_engine):
    mock_get_url.return_value = "postgresql://u:p@localhost:5432/db"
    
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_db_exists.return_value = True

    engine = load.get_engine()

    mock_create_db.assert_not_called()
    assert engine == mock_engine


def test_get_database_url_success(monkeypatch):
    """Testa que a função retorna a URL correta quando todas as variáveis estão definidas."""
    monkeypatch.setenv("POSTGRES_USER", "user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "pass")
    monkeypatch.setenv("POSTGRES_DB", "db")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")

    url = load.get_database_url()
    assert url == "postgresql://user:pass@localhost:5432/db"


def test_get_database_url_missing_env(monkeypatch):
    """Testa que a função lança EnvironmentError quando alguma variável está ausente."""
    # Remove todas as variáveis (ou mantém algumas ausentes)
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("POSTGRES_DB", raising=False)
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.delenv("POSTGRES_PORT", raising=False)

    with pytest.raises(EnvironmentError) as exc:
        load.get_database_url()

    assert "Missing one or more PostgreSQL environment variables" in str(exc.value)
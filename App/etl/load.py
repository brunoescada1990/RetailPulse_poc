import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from App.etl.models import Base
from App.etl.transform import transformed_data

load_dotenv() 

def get_database_url():
    """Cria a string de conexão com base nas variáveis de ambiente."""
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_HOST = os.getenv("POSTGRES_HOST")
    DB_PORT = os.getenv("POSTGRES_PORT")

    if not all([DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT]):
        raise EnvironmentError("❌ Missing one or more PostgreSQL environment variables")

    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    """Cria o engine apenas quando necessário."""
    database_url = get_database_url()

    engine = create_engine(database_url)

    if not database_exists(engine.url):
        print(f"⚙️  A criar base de dados '{engine.url.database}'...")

        create_database(engine.url)
    return create_engine(database_url)


def create_tables():
    """Cria as tabelas no banco de dados."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("✅ Tabelas criadas (ou já existentes).")


def load_data():
    """Carrega os DataFrames transformados para o banco."""
    print("🚀 A carregar dados para PostgreSQL...")

    engine = get_engine()
    data_frames = transformed_data()

    data_frames["customers_df"].to_sql("customers", engine, if_exists="append", index=False)
    data_frames["products_df"].to_sql("products", engine, if_exists="append", index=False)
    data_frames["sales_df"].to_sql("sales", engine, if_exists="append", index=False)
    data_frames["sales_full_df"].to_sql("sales_full", engine, if_exists="append", index=False)

    print("✅ Dados carregados com sucesso!")


if __name__ == "__main__":
    create_tables()
    load_data()
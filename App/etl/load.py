import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base
from transform import transformed_data

load_dotenv() 

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def create_tables():
    Base.metadata.create_all(engine)
    print("✅ Tabelas criadas (ou já existentes).")


def load_data():
    print("🚀 A carregar dados para PostgreSQL...")

    data_frames = transformed_data()

    data_frames["customers_df"].to_sql(
        "customers", engine, if_exists="append", index=False
    )
    data_frames["products_df"].to_sql(
        "products", engine, if_exists="append", index=False
    )

    data_frames["sales_df"].to_sql(
        "sales", engine, if_exists="append", index=False
    )

    data_frames["sales_full_df"].to_sql(
        "sales_full", engine, if_exists="append", index=False
    )

    print("✅ Dados carregados com sucesso!")


if __name__ == "__main__":
    create_tables()
    load_data()
    session.close()
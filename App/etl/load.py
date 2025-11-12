import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from App.etl.models import Base
from App.etl.transform import transformed_data

load_dotenv()

logging.basicConfig(
    filename="App/logs/etl_load.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


def get_database_url() -> str:
    """Build database URL from environment variables."""

    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_HOST = os.getenv("POSTGRES_HOST")
    DB_PORT = os.getenv("POSTGRES_PORT")

    if not all([DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT]):
        log.error("Missing one or more PostgreSQL environment variables")
        raise EnvironmentError("❌ Missing PostgreSQL environment variables")

    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    """Create SQLAlchemy engine and database if it doesn't exist."""

    database_url = get_database_url()
    engine = create_engine(database_url)

    if not database_exists(engine.url):
        log.info(f"⚙️  Creating database '{engine.url.database}'...")
        create_database(engine.url)
        log.info(f"✅ Database '{engine.url.database}' created.")

    return engine


def create_tables():
    """Create tables in the database."""

    engine = get_engine()
    Base.metadata.create_all(engine)
    log.info("✅ Tables created successfully.")


def load_data():
    """Load transformed DataFrames into PostgreSQL."""

    log.info("🚀 Loading data into PostgreSQL...")
    engine = get_engine()

    data_frames = transformed_data()

    for name, df in data_frames.items():
        if df is not None and not df.empty:
            table_name = name.replace("_df", "")
            df.to_sql(
                table_name,
                engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000
            )
            log.info(f"✅ '{table_name}' loaded with {len(df)} rows.")
        else:
            log.warning(f"⚠️ '{name}' is empty or None, skipping.")

    log.info("🚀 Data loaded successfully!")


if __name__ == "__main__":
    create_tables()
    load_data()

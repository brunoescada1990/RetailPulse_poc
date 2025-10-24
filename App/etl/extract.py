import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(
    filename="etl_extract.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

log = logging.getLogger(__name__)

def read_csv_safe(file_path: Path, name: str) -> pd.DataFrame | None:
    """
    Lê um ficheiro CSV de forma segura, com tratamento de erros e logs.
    Retorna um DataFrame ou None se a leitura falhar.
    """
    try:
        df = pd.read_csv(file_path)
        if not df.empty:
            log.info(f"✅ {name} lido com sucesso ({len(df)} linhas).")
        else:
             log.warning(f"✅ {name} lido com sucesso mas não contém dados.")
        
        return df

    except FileNotFoundError:
        log.error(f"❌ File Not Found: {file_path}")
    except pd.errors.EmptyDataError:
        log.error(f"⚠️ File Empty: {file_path}")
    except pd.errors.ParserError:
        log.error(f"⚠️ Error in Parser {file_path}")
    except Exception as e:
        log.exception(f"⚠️ Error in {file_path}: {e}")

    return None


def extract_data():
    base_path = Path(__file__).parent.parent / "data" / "raw"

    customers_df = read_csv_safe(base_path / "customers_info1.csv", "customers_info.csv")
    products_df = read_csv_safe(base_path / "products_info.csv", "products_info.csv")
    sales_df = read_csv_safe(base_path / "sales_raw.csv", "sales_raw.csv")

    return customers_df, products_df, sales_df

            
if __name__ == "__main__":
    customers, products, sales = extract_data()

    print(customers.head())
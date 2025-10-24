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
    Read a CSV file in a safe format, with error handling and logging.
    Returns a DataFrame or None if the read fails.
    """
    try:
        df = pd.read_csv(file_path)
        if not df.empty:
            log.info(f"✅ {name} Read with success ({len(df)} lines).")
        else:
             log.warning(f"✅ {name} Read with success but is Empty.")
        
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

    customers_df = read_csv_safe(base_path / "customers_info.csv", "customers_info.csv")
    products_df = read_csv_safe(base_path / "products_info.csv", "products_info.csv")
    sales_df = read_csv_safe(base_path / "sales_raw.csv", "sales_raw.csv")

    return customers_df, products_df, sales_df

def check_dtypes(df: pd.DataFrame, expected_types: dict, name: str):
    """Check and fix data types"""

    for col, expected_type in expected_types.items():
        if col not in df.columns:
            log.error(f"Column '{col}' not found in {name}.")
            continue

        actual_type = str(df[col].dtype)
        
        if actual_type != expected_type:
            log.warning(f"Column '{col}' IN {name} have type {actual_type}, but was expected {expected_type}. trying convert...")
            
            try:
                if expected_type == "datetime64[ns]":
                    df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")
                
                elif expected_type in ["int64", "float64"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                
                else:
                    df[col] = df[col].astype(str)
                
                log.info(f"'{col}' successfully converted to {expected_type}.")

            except Exception as e:
                log.error(f"Error converting '{col}' in {name}: {e}")
  
    return df

def validate_dataframes(customers, products, sales):
   
    """Valida e corrige os tipos de dados de todos os DataFrames extraídos."""
    
    expected_customers = {
        "customer_id": "int64",
        "first_name": "object",
        "last_name": "object",
        "email": "object",
        "country": "object",
        "birth_date": "datetime64[ns]",
        "gender": "object",
        "loyalty_points": "int64"
    }

    expected_products = {
        "product_id": "int64",
        "product_name": "object",
        "category": "object",
        "price": "float64",
        "stock_quantity": "int64",
        "supplier": "object"
    }

    expected_sales = {
        "sale_id": "int64",
        "customer_id": "int64",
        "product_id": "int64",
        "quantity": "int64",
        "sale_date": "datetime64[ns]",
        "payment_method": "object",
        "store_location": "object"
    }

    validated_data = {}

    if customers is not None:
        validated_data["customers_df"] = check_dtypes(customers, expected_customers, "customers_info")
    if products is not None:
        validated_data["products_df"] = check_dtypes(products, expected_products, "products_info")
    if sales is not None:
        validated_data["sales_df"] = check_dtypes(sales, expected_sales, "sales_raw")
    
    return validated_data

            
if __name__ == "__main__":
    customers, products, sales = extract_data()
    validated = validate_dataframes(customers, products, sales)
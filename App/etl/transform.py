import logging
import re
import json
from datetime import datetime
from typing import Optional, Dict

import pandas as pd
from App.etl.extract import extract_and_validate

# ----------------------------
# Logging configuration
# ----------------------------
logging.basicConfig(
    filename="etl_extract.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")

LOG_PATH_CUSTOMERS = "App/logs/transform_log_customers.txt"
LOG_PATH_PRODUCTS = "App/logs/transform_log_products.txt"
LOG_PATH_SALES = "App/logs/transform_log_sales.txt"

ALLOWED_PAYMENT_METHODS = ["paypal", "crypto", "debit card", "credit card", "cash"]

MIN_DATE = datetime(1900, 1, 1)
MAX_DATE = datetime.now()


def normalize_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize customers dataframe:
    - Drop duplicates and invalid customer_id
    - Format names, countries, gender
    - Validate emails
    - Validate birth dates
    - Correct loyalty points
    Logs summary of changes to JSON file.
    """

    log_dict: Dict[str, int] = {}
    log_dict["date"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    log_dict["init"] = len(df)

    # Remove duplicates and missing customer_id
    df = df.drop_duplicates(subset=["first_name", "last_name", "email", "country", "birth_date"]) \
           .dropna(subset=["customer_id"])
    log_dict["duplicated_rows"] = log_dict["init"] - len(df)

    # Normalize strings
    df["first_name"] = df["first_name"].fillna("UNKNOWN").str.title().str.strip()
    df["last_name"] = df["last_name"].fillna("UNKNOWN").str.title().str.strip()
    df["country"] = df["country"].fillna("UNKNOWN").str.title().str.strip()
    df["gender"] = df["gender"].fillna("Other")

    # Validate emails
    df["email_clean"] = df["email"].astype(str).str.strip().str.lower()
    df["is_email_valid"] = df["email_clean"].apply(lambda x: bool(EMAIL_REGEX.match(x)) if x and x != "nan" else False)
    log_dict["emails_valid"] = int(df["is_email_valid"].sum())
    log_dict["emails_invalid"] = int((~df["is_email_valid"]).sum())
    log_dict["emails_null"] = int(df["email"].isna().sum())

    # Validate birth dates
    df["birth_date_clean"] = df["birth_date"].astype(str).str.strip().str.replace(r"[./]", "-", regex=True)
    df["birth_date_clean"] = pd.to_datetime(df["birth_date_clean"], errors="coerce")
    df["is_birth_date_valid"] = df["birth_date_clean"].apply(lambda d: MIN_DATE <= d <= MAX_DATE if pd.notna(d) else False)
    log_dict["dates_valid"] = int(df["is_birth_date_valid"].sum())
    log_dict["dates_invalid"] = int((~df["is_birth_date_valid"]).sum())
    log_dict["dates_null"] = int(df["birth_date_clean"].isna().sum())

    # Correct loyalty points
    df["loyalty_points"] = pd.to_numeric(df["loyalty_points"], errors="coerce").fillna(0)
    df.loc[df["loyalty_points"] < 0, "loyalty_points"] = 0
    log_dict["points_average"] = float(df["loyalty_points"].mean())
    log_dict["points_min"] = float(df["loyalty_points"].min())
    log_dict["points_max"] = float(df["loyalty_points"].max())

    log_dict["final"] = len(df)
    log_dict["line_removed"] = log_dict["init"] - log_dict["final"]

    with open(LOG_PATH_CUSTOMERS, "a") as f:
        json.dump(log_dict, f, indent=4, default=str)

    return df


def normalize_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize products dataframe:
    - Drop duplicates and invalid product_id
    - Format names, category, supplier
    - Correct price and stock_quantity
    Logs summary of changes to JSON file.
    """

    log_dict: Dict[str, int] = {}
    log_dict["date"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    log_dict["init"] = len(df)

    df = df.drop_duplicates(subset=["product_name", "category", "price", "stock_quantity", "supplier"]) \
           .dropna(subset=["product_id"])

    df["product_name"] = df["product_name"].fillna("UNKNOWN").str.title().str.strip()
    df["category"] = df["category"].fillna("UNKNOWN").str.title().str.strip()
    df["supplier"] = df["supplier"].fillna("UNKNOWN").str.title().str.strip()
    df["price"] = df["price"].fillna(0.0).round(2)
    df["stock_quantity"] = df["stock_quantity"].fillna(0).astype(int)

    log_dict["invalid_prices"] = int(df["price"].isna().sum())
    log_dict["zero_stock"] = int((df["stock_quantity"] == 0).sum())
    log_dict["final"] = len(df)

    with open(LOG_PATH_PRODUCTS, "a") as f:
        json.dump(log_dict, f, indent=4, default=str)

    return df


def normalize_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize sales dataframe:
    - Drop duplicates and invalid sale_id
    - Validate sale_date, payment_method, quantity, customer_id, product_id
    - Add sale_is_valid flag and sales_status_reason
    Logs summary of changes to JSON file.
    """

    log_dict: Dict[str, int] = {}
    log_dict["date"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    log_dict["init"] = len(df)

    df = df.drop_duplicates(subset=["customer_id", "product_id", "quantity", "sale_date", "payment_method", "store_location"]) \
           .dropna(subset=["sale_id"])

    df["sale_is_valid"] = True
    df["sales_status_reason"] = ""

    # Clean and validate dates
    df["sale_date_clean"] = df["sale_date"].astype(str).str.strip().str.replace(r"[./]", "-", regex=True)
    df["sale_date_clean"] = pd.to_datetime(df["sale_date_clean"], errors="coerce")
    mask_invalid_date = df["sale_date_clean"].isna()
    df.loc[mask_invalid_date, "sale_is_valid"] = False
    df.loc[mask_invalid_date, "sales_status_reason"] += "Invalid sale_date; "

    mask_out_of_range = (df["sale_date_clean"] < MIN_DATE) | (df["sale_date_clean"] > MAX_DATE)
    df.loc[mask_out_of_range, "sale_is_valid"] = False
    df.loc[mask_out_of_range, "sales_status_reason"] += "Sale_date out of range; "

    # Validate payment method
    df["payment_method"] = df["payment_method"].astype(str).str.strip().str.lower()
    mask_invalid_payment = ~df["payment_method"].isin(ALLOWED_PAYMENT_METHODS)
    df.loc[mask_invalid_payment, "sale_is_valid"] = False
    df.loc[mask_invalid_payment, "sales_status_reason"] += "Invalid payment method; "
    df["payment_method"] = df["payment_method"].str.title()

    # Validate quantity
    mask_invalid_quantity = (df["quantity"].isna()) | (df["quantity"] <= 0)
    df.loc[mask_invalid_quantity, "sale_is_valid"] = False
    df.loc[mask_invalid_quantity, "sales_status_reason"] += "Invalid quantity; "

    # Validate customer_id and product_id
    mask_invalid_customer_id = df["customer_id"].isna() | (df["customer_id"] == 0)
    df.loc[mask_invalid_customer_id, "sale_is_valid"] = False
    df.loc[mask_invalid_customer_id, "sales_status_reason"] += "Invalid customer_id; "

    mask_invalid_product_id = df["product_id"].isna() | (df["product_id"] == 0)
    df.loc[mask_invalid_product_id, "sale_is_valid"] = False
    df.loc[mask_invalid_product_id, "sales_status_reason"] += "Invalid product_id; "

    log_dict["final"] = len(df)
    log_dict["line_removed"] = log_dict["init"] - log_dict["final"]
    log_dict["valid"] = int(df["sale_is_valid"].sum())
    log_dict["invalid"] = int((~df["sale_is_valid"]).sum())

    with open(LOG_PATH_SALES, "a") as f:
        json.dump(log_dict, f, indent=4, default=str)

    return df


def transform_dates_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract year, month, and day from sale_date_clean.
    """

    df["sale_date_clean"] = pd.to_datetime(df["sale_date_clean"], errors="coerce")
    df["sale_year"] = df["sale_date_clean"].dt.year.astype("Int64")
    df["sale_month"] = df["sale_date_clean"].dt.month.astype("Int64")
    df["sale_day"] = df["sale_date_clean"].dt.day.astype("Int64")
    return df


def create_sales_full(df_sales: pd.DataFrame, df_products: pd.DataFrame, df_customers: pd.DataFrame) -> pd.DataFrame:
    """
    Merge sales, products, and customers into a full sales dataframe.
    Computes total_value_sales considering quantity, price, and validity.
    """

    df_sales_full = pd.merge(df_sales, df_products, on="product_id", how="left")
    df_sales_full = pd.merge(df_sales_full, df_customers, on="customer_id", how="left")
    df_sales_full["total_value_sales"] = (
        (df_sales_full["quantity"].fillna(0) * df_sales_full["price"].fillna(0)) *
        df_sales_full["sale_is_valid"].astype(int)
    ).abs()
    return df_sales_full


def transformed_data() -> Dict[str, pd.DataFrame]:
    """
    Orchestrate extraction and transformation of all dataframes.
    Returns a dictionary of transformed dataframes.
    """
    
    dataframes = extract_and_validate()
    dataframe_transformed: Dict[str, pd.DataFrame] = {}

    dataframe_transformed["customers_df"] = normalize_customers(dataframes.get("customers_df"))
    dataframe_transformed["products_df"] = normalize_products(dataframes.get("products_df"))
    dataframe_transformed["sales_df"] = transform_dates_sales(normalize_sales(dataframes.get("sales_df")))

    dataframe_transformed["sales_full_df"] = create_sales_full(
        dataframe_transformed.get("sales_df"),
        dataframe_transformed.get("products_df"),
        dataframe_transformed.get("customers_df")
    )

    log.info("Transformation completed successfully.")
    return dataframe_transformed


if __name__ == "__main__":
    transformed = transformed_data()

import logging
import pandas as pd
import re
import json

from datetime import datetime
from App.etl.extract import extract_and_validate

logging.basicConfig(
    filename="etl_extract.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

log = logging.getLogger(__name__)
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
log_path_customers = "App/logs/transform_log_customers.txt"
log_path_products = "App/logs/transform_log_products.txt"
log_path_sales = "App/logs/transform_log_sales.txt"


def normalize_customers(df: pd.DataFrame):

    log = {}
    
    log["date"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    log["init"] = len(df)

    df = df.drop_duplicates(subset=["first_name", "last_name", "email", "country", "birth_date"]).dropna(subset=["customer_id"])

    log["duplicated_rows"] = log["init"] - len(df)

    
    df["first_name"] = df["first_name"].fillna("UNKNOWN").str.title().str.strip()
    df["last_name"] = df["last_name"].fillna("UNKNOWN").str.title().str.strip()
    df["country"] = df["country"].fillna("UNKNOWN").str.title().str.strip()
    df["gender"] = df["gender"].fillna("Other")
   
    df["email_clean"] = (df["email"].astype(str).str.strip().str.lower())
    df["is_email_valid"] = df["email_clean"].apply(lambda x: bool(EMAIL_REGEX.match(x)) if x and x != "nan" else False)

    log["emails_valids"] = int(df["is_email_valid"].sum())
    log["emails_invalids"] = int((~df["is_email_valid"]).sum())
    log["emails_nulls"] = int(df["email"].isna().sum())

    min_date = datetime(1900, 1, 1)
    max_date = datetime.now()
    df["birth_date_clean"] = (
    df["birth_date"].astype(str).str.strip().str.replace(r"[./]", "-", regex=True))
    df["birth_date_clean"] = pd.to_datetime(df["birth_date_clean"], errors="coerce", format="mixed")
    df["is_birth_date_valid"] = df["birth_date_clean"].apply(lambda d: min_date <= d <= max_date if pd.notna(d) else False)

    log["dates_valid"] = int(df["is_birth_date_valid"].sum())
    log["dates_invalids"] = int((~df["is_birth_date_valid"]).sum())
    log["dates_nulls"] = int(df["birth_date_clean"].isna().sum())

    df["loyalty_points"] = pd.to_numeric(df["loyalty_points"], errors="coerce").fillna(0)
    df.loc[df["loyalty_points"] < 0, "loyalty_points"] = 0

    log["points_average"] = float(df["loyalty_points"].mean())
    log["points_min"] = float(df["loyalty_points"].min())
    log["points_max"] = float(df["loyalty_points"].max())

    log["final"] = len(df)

    log["line_removed"] = log["init"] - log["final"]

    with open(log_path_customers, "a") as f:
        json.dump(log, f, indent=4)

    return df

def normalize_products(df: pd.DataFrame):
    
    log = {}
    
    log["date"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    log["init"] = len(df)

    df = df.drop_duplicates(subset=["product_name","category","price","stock_quantity","supplier"]).dropna(subset=["product_id"])

    df["product_name"] = df["product_name"].fillna("UNKNOWN").str.title().str.strip()
    df["category"] = df["category"].fillna("UNKNOWN").str.title().str.strip()
    df["supplier"] = df["supplier"].fillna("UNKNOWN").str.title().str.strip()
    df["price"] = df["price"].fillna(0.0).round(2)
    df["stock_quantity"] = df["stock_quantity"].fillna(0).astype(int)


    log["invalid_prices"] = df["price"].isna().sum().astype(int)
    log["zero_stock"] = (df["stock_quantity"] == 0).sum().astype(int)
    log["final"] = len(df)


    with open(log_path_products, "a") as f:
        json.dump(log, f, indent=4, default=str)

    return df

def normalize_sales(df: pd.DataFrame):

    log = {}
    
    log["date"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    log["init"] = len(df)

    df = df.drop_duplicates(subset=["customer_id", "product_id", "quantity", "sale_date", "payment_method", "store_location"]).dropna(subset=["sale_id"])

    df["sale_is_valid"] = True
    df["sales_status_reason"] = ""

    min_date = datetime(1900, 1, 1)
    max_date = datetime.now()
    df["sale_date_clean"] = (df["sale_date"].astype(str).str.strip().str.replace(r"[./]", "-", regex=True))
    df["sale_date_clean"] = pd.to_datetime(df["sale_date_clean"], errors="coerce", format="mixed")
    mask_invalid_date = df["sale_date_clean"].isna()
    df.loc[mask_invalid_date, "sale_is_valid"] = False
    df.loc[mask_invalid_date, "sales_status_reason"] += "Invalid sale_date; "

    mask_out_of_range = (df["sale_date_clean"] < min_date) | (df["sale_date_clean"] > max_date)
    df.loc[mask_out_of_range, "sales_status_reason"] += "Invalid sale_date out of interval; "
    df.loc[mask_out_of_range, "sale_is_valid"] = False
    
    allowed_methods = ["paypal", "crypto", "debit card", "credit card", "cash"]
    df["payment_method"]=df["payment_method"].astype(str).str.strip().str.lower()
    mask_invalid_payment = ~df["payment_method"].isin(allowed_methods)
    df.loc[mask_invalid_payment, "sale_is_valid"] = False
    df.loc[mask_invalid_payment, "sales_status_reason"] += "Invalid payment method; "
    df["payment_method"]= df["payment_method"].str.title()

    mask_invalid_quantity = (df["quantity"].isna()) | (df["quantity"] <= 0)
    df.loc[mask_invalid_quantity, "sale_is_valid"] = False
    df.loc[mask_invalid_quantity, "sales_status_reason"] += "Quantity invalid; "
    
    mask_invalid_customer_id = df["customer_id"].isna() | (df["customer_id"] == 0)
    df.loc[mask_invalid_customer_id, "sale_is_valid"] = False
    df.loc[mask_invalid_customer_id, "sales_status_reason"] += "Customer ID invalid; "

    mask_invalid_product_id = df["product_id"].isna() | (df["product_id"] == 0)
    df.loc[mask_invalid_product_id, "sale_is_valid"] = False
    df.loc[mask_invalid_product_id, "sales_status_reason"] += "Product ID invalid; "

    log["final"] = len(df)

    log["line_removed"] = log["init"] - log["final"]

    log["valid"] = int(df["sale_is_valid"].sum())
    log["invalid"] = int((~df["sale_is_valid"]).sum())


    with open(log_path_sales, "a") as f:
        json.dump(log, f, indent=4, default=str)

    return df

def transform_dates_sales(df: pd.DataFrame):

    df["sale_date_clean"] = pd.to_datetime(df["sale_date_clean"], errors="coerce")
    df["sale_year"] = df["sale_date_clean"].dt.year.astype("Int64")
    df["sale_month"] = df["sale_date_clean"].dt.month.astype("Int64")
    df["sale_day"] = df["sale_date_clean"].dt.day.astype("Int64")

    return df


def create_sales_full (df_sales: pd.DataFrame, df_products: pd.DataFrame, df_customers: pd.DataFrame):
    
    df_sales_full = pd.merge(df_sales, df_products, on="product_id", how="left")
    df_sales_full = pd.merge(df_sales_full, df_customers, on="customer_id", how="left")

    df_sales_full["total_value_sales"] = ((df_sales_full["quantity"].fillna(0) * df_sales_full["price"].fillna(0)) * df_sales_full["sale_is_valid"].astype(int)).abs()

    return df_sales_full


def transformed_data():

    dataframes = extract_and_validate()
    dataframe_trasformated = {}

    dataframe_trasformated["customers_df"] = normalize_customers(dataframes.get("customers_df"))
    dataframe_trasformated["products_df"] = normalize_products(dataframes.get("products_df"))
    dataframe_trasformated["sales_df"] = transform_dates_sales(normalize_sales(dataframes.get("sales_df")))

    dataframe_trasformated["sales_full_df"] = create_sales_full(dataframe_trasformated.get("sales_df"), dataframe_trasformated.get("products_df"), dataframe_trasformated.get("customers_df"))

    print(dataframe_trasformated.get("products_df").head())

    return dataframe_trasformated

if __name__ == "__main__":
   transform = transformed_data()
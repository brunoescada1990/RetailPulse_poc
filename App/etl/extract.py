import pandas as pd
from pathlib import Path

def extract_data():
    base_path = Path(__file__).parent.parent / "data" / "raw"
    customers_df = pd.read_csv(base_path / "customers_info.csv")
    products_df = pd.read_csv(base_path / "products_info.csv")
    sales_df = pd.read_csv(base_path / "sales_raw.csv")

    return customers_df, products_df, sales_df

if __name__ == "__main__":
    customers, products, sales = extract_data()
    print(customers.head())
    print(products.head())
    print(sales.head())
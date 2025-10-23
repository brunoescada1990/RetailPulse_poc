import pandas as pd
from pathlib import Path

def extract_data():
    base_path = Path(__file__).parent.parent / "data" / "raw"
    customers = pd.read_csv(base_path / "customers_info.csv")
    products = pd.read_csv(base_path / "products_info.csv")
    sales = pd.read_csv(base_path / "sales_raw.csv")

    return customers, products, sales

if __name__ == "__main__":
    customers, products, sales = extract_data()
    print(customers.head())
    print(products.head())
    print(sales.head())
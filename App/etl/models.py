from sqlalchemy import Column, Integer, String, Date, Numeric, ForeignKey, Boolean
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Customer(Base):
    __tablename__ = "customers"
    
    customer_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(100))
    last_name = Column(String(100))
    email = Column(String(150))
    country = Column(String(100))
    birth_date = Column(Date)
    gender = Column(String(20))
    loyalty_points = Column(Integer)
    
    email_clean = Column(String(150))
    is_email_valid = Column(Boolean)
    birth_date_clean = Column(Date)
    is_birth_date_valid = Column(Boolean)
    
    sales = relationship("Sale", back_populates="customer")


class Product(Base):
    __tablename__ = "products"
    
    product_id = Column(Integer, primary_key=True, autoincrement=True)
    product_name = Column(String(100))
    category = Column(String(50))
    price = Column(Numeric(10, 2))
    stock_quantity = Column(Integer)
    supplier = Column(String(100))
    
    sales = relationship("Sale", back_populates="product")


class Sale(Base):
    __tablename__ = "sales"
    
    sale_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, ForeignKey("customers.customer_id"))
    product_id = Column(Integer, ForeignKey("products.product_id"))
    
    quantity = Column(Integer)
    sale_date = Column(Date)
    payment_method = Column(String(50))
    store_location = Column(String(50))
    sale_is_valid = Column(Boolean)
    sales_status_reason = Column(String(255))
    sale_date_clean = Column(Date)
    sale_year = Column(Integer)
    sale_month = Column(Integer)
    sale_day = Column(Integer)
    
    customer = relationship("Customer", back_populates="sales")
    product = relationship("Product", back_populates="sales")


class SalesFull(Base):
    __tablename__ = "sales_full"
    
    sale_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer)
    product_id = Column(Integer)
    
    quantity = Column(Integer)
    sale_date = Column(Date)
    payment_method = Column(String(50))
    store_location = Column(String(100))
    sale_is_valid = Column(Boolean)
    sales_status_reason = Column(String(255))
    
    sale_date_clean = Column(Date)
    sale_year = Column(Integer)
    sale_month = Column(Integer)
    sale_day = Column(Integer)
    
    product_name = Column(String(100))
    category = Column(String(50))
    price = Column(Numeric(10, 2))
    stock_quantity = Column(Integer)
    supplier = Column(String(100))
    
    first_name = Column(String(100))
    last_name = Column(String(100))
    email = Column(String(150))
    country = Column(String(100))
    birth_date = Column(Date)
    gender = Column(String(20))
    loyalty_points = Column(Integer)
    
    email_clean = Column(String(150))
    is_email_valid = Column(Boolean)
    birth_date_clean = Column(Date)
    is_birth_date_valid = Column(Boolean)
    
    total_value_sales = Column(Numeric(12, 2))

# Import library yang dibutuhkan
import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine
import datetime
import logging
import psycopg2

# Setup logging untuk mencatat proses ETL
logging.basicConfig(level=logging.INFO, filename='etl_log.log', format='%(asctime)s - %(levelname)s - %(message)s')

# String koneksi untuk OLTP dan Staging
OLTP_CONNECTION = "postgresql+psycopg2://postgres:chriscakra15@localhost:5432/Adventureworks"
STG_CONNECTION = "postgresql+psycopg2://postgres:chriscakra15@localhost:5432/staging"

# Validasi apakah database staging tersedia
try:
    conn = psycopg2.connect("dbname=staging user=postgres password=chriscakra15 host=localhost port=5432")
    conn.close()
except psycopg2.OperationalError as e:
    logging.error("Database 'staging' tidak ditemukan. Harap buat database 'staging' terlebih dahulu.")
    raise SystemExit("Database staging tidak ditemukan.")

# Membuat koneksi engine SQLAlchemy untuk OLTP dan Staging
engine_oltp = create_engine(OLTP_CONNECTION)
engine_staging = create_engine(STG_CONNECTION)

logging.info("Mulai proses ETL")

# --- Extraction ---
# Mengambil data dari tabel yang relevan dari OLTP
df_sales = pd.read_sql("SELECT SalesOrderID, OrderDate, CustomerID, SalesPersonID, SubTotal FROM Sales.SalesOrderHeader", engine_oltp)
df_customers = pd.read_sql("SELECT CustomerID, PersonID FROM Sales.Customer WHERE PersonID IS NOT NULL", engine_oltp)
df_persons = pd.read_sql("SELECT * FROM Person.Person", engine_oltp)
df_employees = pd.read_sql("SELECT BusinessEntityID, JobTitle, Gender FROM HumanResources.Employee", engine_oltp)
df_products = pd.read_sql("SELECT ProductID, Name, ProductSubcategoryID, StandardCost, ListPrice FROM Production.Product", engine_oltp)

# --- Transformation ---
# Konversi nama kolom menjadi huruf kecil untuk konsistensi
df_sales.columns = df_sales.columns.str.lower()
df_customers.columns = df_customers.columns.str.lower()
df_persons.columns = df_persons.columns.str.lower()

# Membuat dimensi customer dengan menggabungkan customer dan person
dim_customer = df_customers.merge(df_persons, left_on='personid', right_on='businessentityid')
dim_customer = dim_customer[['customerid', 'firstname', 'lastname']].drop_duplicates()
dim_customer['customerkey'] = dim_customer.index + 1  # Menambahkan surrogate key

# Membuat dimensi waktu dari kolom orderdate
df_sales['orderdate'] = pd.to_datetime(df_sales['orderdate'])
dim_time = df_sales[['orderdate']].drop_duplicates()
dim_time['year'] = dim_time['orderdate'].dt.year
dim_time['quarter'] = dim_time['orderdate'].dt.quarter
dim_time['month'] = dim_time['orderdate'].dt.month
dim_time['day'] = dim_time['orderdate'].dt.day
dim_time['weekday'] = dim_time['orderdate'].dt.dayofweek
dim_time['timekey'] = dim_time.index + 1  # Menambahkan surrogate key

# Membuat dimensi produk dari tabel produk
df_products.columns = df_products.columns.str.lower()
dim_product = df_products[['productid', 'name', 'productsubcategoryid']].drop_duplicates()
dim_product['productkey'] = dim_product.index + 1  # Menambahkan surrogate key

# Membuat tabel fakta penjualan dengan menggabungkan dengan dimensi waktu
fact_sales = df_sales.merge(dim_time, on='orderdate')
fact_sales['salesfactid'] = fact_sales.index + 1  # Menambahkan surrogate key
fact_sales = fact_sales[['salesfactid', 'salesorderid', 'salespersonid', 'customerid', 'timekey', 'subtotal']]

# --- Load ---
# Menyimpan dimensi dan tabel fakta ke database staging
try:
    dim_customer.to_sql("DimCustomer", con=engine_staging, if_exists='replace', index=False)
    dim_time.to_sql("DimTime", con=engine_staging, if_exists='replace', index=False)
    dim_product.to_sql("DimProduct", con=engine_staging, if_exists='replace', index=False)
    fact_sales.to_sql("FactSales", con=engine_staging, if_exists='replace', index=False)
    logging.info("ETL selesai tanpa error dan data dimuat ke database staging")
except Exception as e:
    logging.error(f"Gagal melakukan ETL: {e}")
    raise

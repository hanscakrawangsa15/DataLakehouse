import pandas as pd
from sqlalchemy import create_engine
import datetime

# create a connection to the database
oltp_connection = "postgresql+psycopg2://postgres:Metta30@localhost:5432/Adventureworks"
stg_connection = "postgresql+psycopg2://postgres:Metta30@localhost:5432/Staging"

engine_oltp = create_engine(oltp_connection)
engine_stg = create_engine(stg_connection)  # fixed typo in variable name

# --- step 1: extract data from oltp ---

# query 1: production total for each vendor
vendor_production_df = pd.read_sql("""
    select v.name as vendorname, sum(pod.orderqty) as totalproduction,
        v.businessentityid as vendorid, poh.purchaseorderid
    from purchasing.vendor v
    join purchasing.purchaseorderheader poh on v.businessentityid = poh.vendorid
    join purchasing.purchaseorderdetail pod on poh.purchaseorderid = pod.purchaseorderid
    join production.product p on pod.productid = p.productid
    group by v.name, v.businessentityid, poh.purchaseorderid
    order by totalproduction desc
""", engine_oltp)

# query 2: wage for every person per hour
employee_wage_df = pd.read_sql("""
    select e.businessentityid, p.firstname, p.lastname, eph.rate as hourlywage
    from humanresources.employee e
    join person.person p on e.businessentityid = p.businessentityid
    join humanresources.employeepayhistory eph on e.businessentityid = eph.businessentityid
    where eph.payfrequency = 1
    order by hourlywage
""", engine_oltp)

# query 3: total price of bom for each product
product_bom_df = pd.read_sql("""
    select p.name as productname, p.productid,
           sum(bom.perassemblyqty * comp.standardcost) as totalbomcost
    from production.product p
    join production.billofmaterials bom on p.productid = bom.productassemblyid
    join production.product comp on bom.componentid = comp.productid
    group by p.name, p.productid
    order by totalbomcost desc
""", engine_oltp)

# extract additional dimension data
# extract date dimension from purchaseorderheader
date_df = pd.read_sql("""
    select distinct orderdate 
    from purchasing.purchaseorderheader
""", engine_oltp)

# extract product dimension
product_df = pd.read_sql("""
    select productid, name, productnumber, standardcost, listprice, productsubcategoryid
    from production.product
""", engine_oltp)

# extract vendor dimension
vendor_df = pd.read_sql("""
    select businessentityid as vendorid, name, accountnumber, creditrating
    from purchasing.vendor
""", engine_oltp)

# extract purchase order details for fact table
purchase_order_df = pd.read_sql("""
    select pod.purchaseorderid, pod.purchaseorderdetailid, pod.productid, 
           pod.orderqty, pod.unitprice, poh.vendorid, poh.orderdate
    from purchasing.purchaseorderdetail pod
    join purchasing.purchaseorderheader poh on pod.purchaseorderid = poh.purchaseorderid
""", engine_oltp)

# --- step 2: transform the data ---

# transform date dimension
date_df['datekey'] = pd.to_datetime(date_df['orderdate']).dt.strftime('%Y%m%d').astype(int)
date_df['year'] = pd.to_datetime(date_df['orderdate']).dt.year
date_df['month'] = pd.to_datetime(date_df['orderdate']).dt.month
date_df['day'] = pd.to_datetime(date_df['orderdate']).dt.day
date_df['quarter'] = pd.to_datetime(date_df['orderdate']).dt.quarter

# add datekey to purchase_order_df
purchase_order_df['datekey'] = pd.to_datetime(purchase_order_df['orderdate']).dt.strftime('%Y%m%d').astype(int)

# create fact table for vendor production
fact_vendor_production = vendor_production_df[['vendorid', 'purchaseorderid', 'totalproduction']]

# create fact table for product bom cost
fact_product_bom = product_bom_df[['productid', 'totalbomcost']]

# create fact table for purchases with date and product dimensions
fact_purchases = purchase_order_df[['purchaseorderid', 'purchaseorderdetailid', 'productid', 
                                    'vendorid', 'datekey', 'orderqty', 'unitprice']]
fact_purchases['totalcost'] = fact_purchases['orderqty'] * fact_purchases['unitprice']

# add load timestamp to all dimension and fact tables
load_time = datetime.datetime.now()

# add load_date to dimension tables
vendor_df['load_date'] = load_time
product_df['load_date'] = load_time
employee_wage_df['load_date'] = load_time
date_df['load_date'] = load_time
product_bom_df['load_date'] = load_time

# add load_date to fact tables
fact_vendor_production['load_date'] = load_time
fact_product_bom['load_date'] = load_time
fact_purchases['load_date'] = load_time

# --- step 3: load data to staging ---
vendor_df.to_sql("vendor", engine_stg, if_exists='replace', index=False)
product_df.to_sql("product", engine_stg, if_exists='replace', index=False)
employee_wage_df.to_sql("employee", engine_stg, if_exists='replace', index=False)
date_df.to_sql("date", engine_stg, if_exists='replace', index=False)

fact_vendor_production.to_sql("vendor_production", engine_stg, if_exists='replace', index=False)
fact_product_bom.to_sql("product_bom", engine_stg, if_exists='replace', index=False)
fact_purchases.to_sql("fact_purchases", engine_stg, if_exists='replace', index=False)

print("etl process to postgresql staging completed successfully.")
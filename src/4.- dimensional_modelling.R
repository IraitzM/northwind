# Let's explore how to model our retail database
# First we will need to import required libraries to interact with our local PostgreSQL RDBMS
#install.packages("pacman")
library(pacman)

# Auxiliary functions
source('src/aux_functions.R')

# Software
# PostgreSQL

# Could be used Docker for this:
# 1. docker run --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres:13
# 2. docker inspect some-postgres
dhost <- 'localhost'

# Let's connect to our database
con <- DBI::dbConnect(PostgreSQL(), 
                      user= 'postgres', 
                      password = 'mysecretpassword', 
                      host=dhost)

# List existing tables
list(con)

# New schema
exec(con, "CREATE SCHEMA analytics")

# Now we will create some dimensions from existing data. This is a simple way to take both the schema and the data in one sentence
exec(con, "CREATE TABLE analytics.customer_dim AS SELECT customerid, companyname, region, city FROM customers")
exec(con, "CREATE TABLE analytics.order_dim AS SELECT orderid, orderdate, shipvia, freight FROM orders")
exec(con, "CREATE TABLE analytics.product_dim AS SELECT productid, productname, unitprice, unitsinstock FROM products")

# And add corresponding primary keys
exec(con, 'ALTER TABLE ONLY analytics.customer_dim ADD CONSTRAINT pk_customer PRIMARY KEY (customerid);')
exec(con, 'ALTER TABLE ONLY analytics.product_dim ADD CONSTRAINT pk_product PRIMARY KEY (productid);')
exec(con, 'ALTER TABLE ONLY analytics.order_dim ADD CONSTRAINT pk_order PRIMARY KEY (orderid);')

# With that create our fact table for orderline
exec(con, "CREATE TABLE analytics.orderline_fact AS SELECT od.*, customerid, orderdate FROM orders o INNER JOIN order_details od ON od.orderid = o.orderid")
exec(con, 'ALTER TABLE ONLY analytics.orderline_fact ADD CONSTRAINT pk_olfact PRIMARY KEY (customerid, productid, orderid);')

# And relate facts table with dimension tables
exec(con, 'ALTER TABLE ONLY analytics.orderline_fact ADD CONSTRAINT fk_olfact_customers FOREIGN KEY (customerid) REFERENCES analytics.customer_dim;')
exec(con, 'ALTER TABLE ONLY analytics.orderline_fact ADD CONSTRAINT fk_olfact_orders FOREIGN KEY (orderid) REFERENCES analytics.order_dim;')
exec(con, 'ALTER TABLE ONLY analytics.orderline_fact ADD CONSTRAINT fk_olfact_products FOREIGN KEY (productid) REFERENCES analytics.product_dim;')

# List existing tables
list(con)

con <- DBI::dbConnect(PostgreSQL(), 
                      user= 'postgres', 
                      password = 'mysecretpassword',
                      options="-c search_path=analytics",
                      host=dhost)

# Great now we could ask for things metrics, like for example...
df <- query(con, "SELECT date_part('month', orderdate) AS month, customerid AS client, productid AS product, count(*) AS howmany FROM orderline_fact GROUP BY month, client, product")
View(df)

# ... and add information from dimensions when needed 
df <- query(con, "SELECT date_part('month', orderdate) AS month, customerid AS client, p.productname AS product, count(*) AS howmany FROM orderline_fact olf JOIN product_dim p ON p.productid = olf.productid GROUP BY month, client, product")
View(df)

# Disconnect
dbDisconnect(con)

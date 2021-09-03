# Let's manipulate some data
# First we will need to import required libraries to interact with our local PostgreSQL RDBMS
renv::restore()

# Auxiliary functions
source('src/aux_functions.R')

# Software
# PostgreSQL

# Could be used Docker for this:
# 1. docker restart some-postgres
# 3. docker inspect some-postgres
dhost <- 'localhost'

# Let's connect to our database
con <- DBI::dbConnect(RPostgres::Postgres(), 
                      user= 'postgres', 
                      password = 'mysecretpassword', 
                      host=dhost)

# List existing tables
list(con)

# Now we will create some dimensions from existing data. This is a simple way to take both the schema and the data in one sentence
query(con, "CREATE TABLE customer_dim AS SELECT customerid, companyname, region, city FROM customers")
query(con, "CREATE TABLE order_dim AS SELECT orderid, orderdate, shipvia, freight FROM orders")
query(con, "CREATE TABLE product_dim AS SELECT productid, productname, unitprice, unitsinstock FROM products")

# And add corresponding primary keys
exec(con, 'ALTER TABLE ONLY customer_dim ADD CONSTRAINT pk_customer PRIMARY KEY (customerid);')
exec(con, 'ALTER TABLE ONLY product_dim ADD CONSTRAINT pk_product PRIMARY KEY (productid);')
exec(con, 'ALTER TABLE ONLY order_dim ADD CONSTRAINT pk_order PRIMARY KEY (orderid);')

# With that create our fact table for orderline
query(con, "CREATE TABLE orderline_fact AS SELECT od.*, customerid, orderdate FROM orders o INNER JOIN order_details od ON od.orderid = o.orderid")
exec(con, 'ALTER TABLE ONLY orderline_fact ADD CONSTRAINT pk_olfact PRIMARY KEY (customerid, productid, orderid);')

# And relate facts table with dimension tables
exec(con, 'ALTER TABLE ONLY orderline_fact ADD CONSTRAINT fk_olfact_customers FOREIGN KEY (customerid) REFERENCES customer_dim;')
exec(con, 'ALTER TABLE ONLY orderline_fact ADD CONSTRAINT fk_olfact_orders FOREIGN KEY (orderid) REFERENCES order_dim;')
exec(con, 'ALTER TABLE ONLY orderline_fact ADD CONSTRAINT fk_olfact_products FOREIGN KEY (productid) REFERENCES product_dim;')

# List existing tables
list(con)

# Great now we could ask for things metrics, like for example...
df <- query(con, "SELECT date_part('month', orderdate) AS month, customerid AS client, productid AS product, count(*) AS howmany FROM orderline_fact GROUP BY month, client, product")

# ... and add information from dimensions when needed 
df <- query(con, "SELECT date_part('month', orderdate) AS month, customerid AS client, p.productname AS product, count(*) AS howmany FROM orderline_fact olf JOIN product_dim p ON p.productid = olf.productid GROUP BY month, client, product")
View(df)

# Disconnect
dbDisconnect(con)

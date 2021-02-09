# Let's explore how to model our retail database
# First we will need to import required libraries to interact with our local PostgreSQL RDBMS
renv::restore()

# Auxiliary functions
source('src/aux_functions.R')

# Software
# PostgreSQL

# Could be used Docker for this:
# 1. docker run --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres:13
# 2. docker inspect some-postgres
dhost <- '172.17.0.2'

# Let's connect to our database
con <- DBI::dbConnect(RPostgres::Postgres(), 
                      user= 'postgres', 
                      password = 'mysecretpassword', 
                      host=dhost)

# List existing tables
list(con)

# Create relations #############################################################

# Customers
q <- 'CREATE TABLE customers (
    customerid bpchar NOT NULL,
    companyname character varying(40) NOT NULL,
    contactname character varying(30),
    contacttitle character varying(30),
    address character varying(60),
    city character varying(30),
    region character varying(30),
    postalcode character varying(10),
    country character varying(30),
    phone character varying(24),
    fax character varying(24)
);'
exec(con,q)

# Employees
q <- 'CREATE TABLE employees (
    employeeid smallint NOT NULL,
    lastname character varying(20) NOT NULL,
    firstname character varying(10) NOT NULL,
    title character varying(30),
    titleofcourtesy character varying(25),
    birthdate date,
    hiredate date,
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postalcode character varying(10),
    country character varying(15),
    homephone character varying(24),
    extension character varying(4),
    photo bytea,
    notes text,
    reportsto smallint,
    photopath character varying(255)
);'
exec(con, q)

# Order details
q <- 'CREATE TABLE order_details (
    orderid smallint NOT NULL,
    productid smallint NOT NULL,
    unitprice real NOT NULL,
    quantity smallint NOT NULL,
    discount real NOT NULL
);'
exec(con, q)

# Orders
q <- 'CREATE TABLE orders (
    orderid smallint NOT NULL,
    customerid bpchar,
    employeeid smallint,
    orderdate date,
    requireddate date,
    shippeddate date,
    shipvia smallint,
    freight real
);'
exec(con, q)

# Products
q <- 'CREATE TABLE products (
    productid smallint NOT NULL,
    productname character varying(40) NOT NULL,
    supplierid smallint,
    categoryid smallint,
    quantityperunit character varying(20),
    unitprice real,
    unitsinstock smallint,
    unitsonorder smallint,
    reorderlevel smallint,
    discontinued integer NOT NULL
);'
exec(con, q)

# List existing tables
list(con)

# Constraints ##################################################################
# Add PK to customers
q <-'ALTER TABLE ONLY customers
ADD CONSTRAINT pk_customers PRIMARY KEY (customerid);'
exec(con, q)

# Add PK to employees
q <-'ALTER TABLE ONLY employees
ADD CONSTRAINT pk_employees PRIMARY KEY (employeeid);'
exec(con, q)

# Add composed PK to order_details
q <-'ALTER TABLE ONLY order_details
ADD CONSTRAINT pk_order_details PRIMARY KEY (orderid, productid);'
exec(con, q)

# Add PK to orders
q <-'ALTER TABLE ONLY orders
ADD CONSTRAINT pk_orders PRIMARY KEY (orderid);'
exec(con, q)

# Add PK to products
q <-'ALTER TABLE ONLY products
ADD CONSTRAINT pk_products PRIMARY KEY (productid);'
exec(con, q)

# Now relate FKs orders.customerid with customers
q <-'ALTER TABLE ONLY orders
ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customerid) REFERENCES customers;'
exec(con, q)

# Now relate FKs orders.employeeid with employees
q <-'ALTER TABLE ONLY orders
ADD CONSTRAINT fk_orders_employees FOREIGN KEY (employeeid) REFERENCES employees;'
exec(con, q)

# Now relate FKs order_details.productid with products
q <-'ALTER TABLE ONLY order_details
ADD CONSTRAINT fk_order_details_products FOREIGN KEY (productid) REFERENCES products;'
exec(con, q)

# Now relate FKs order_details.orderid with orders
q <-'ALTER TABLE ONLY order_details
ADD CONSTRAINT fk_order_details_orders FOREIGN KEY (orderid) REFERENCES orders;'
exec(con, q)

# Disconnect from the database
dbDisconnect(con)

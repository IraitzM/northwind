# Let's explore how to model our retail database
# First we will need to import required libraries to interact with our local PostgreSQL RDBMS
renv::restore()

# Auxiliary functions
source('src/aux_functions.R')

# Software
# PostgreSQL

# Could be used Docker for this:
# 1. docker pull postgres
# 2. docker run --name some-postgres -e POSTGRES_PASSWORD=postgres -d postgres
# 3. docker inspect some-postgres
dhost <- '172.17.0.2'

# Let's connect to our database
con <- DBI::dbConnect(RPostgres::Postgres(), 
                      user= 'postgres', 
                      password = 'postgres', 
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

# Show the tables
show(con)

# Constraints ##################################################################
# Add PK to customers
q <-'ALTER TABLE ONLY customers
ADD CONSTRAINT pk_customers PRIMARY KEY (customerid);'
exec(con, q)
  
q <-'ALTER TABLE ONLY employees
ADD CONSTRAINT pk_employees PRIMARY KEY (employeeid);'
exec(con, q)

q <-'ALTER TABLE ONLY order_details
ADD CONSTRAINT pk_order_details PRIMARY KEY (orderid, productid);'
exec(con, q)
  
q <-'ALTER TABLE ONLY orders
ADD CONSTRAINT pk_orders PRIMARY KEY (orderid);'
exec(con, q)
  
q <-'ALTER TABLE ONLY products
ADD CONSTRAINT pk_products PRIMARY KEY (productid);'
exec(con, q)

q <-'ALTER TABLE ONLY orders
ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customerid) REFERENCES customers;'
exec(con, q)

q <-'ALTER TABLE ONLY orders
ADD CONSTRAINT fk_orders_employees FOREIGN KEY (employeeid) REFERENCES employees;'
exec(con, q)

q <-'ALTER TABLE ONLY order_details
ADD CONSTRAINT fk_order_details_products FOREIGN KEY (productid) REFERENCES products;'
exec(con, q)

q <-'ALTER TABLE ONLY order_details
ADD CONSTRAINT fk_order_details_orders FOREIGN KEY (orderid) REFERENCES orders;'
exec(con, q)

# Show it
show(con)

# Get data in ##################################################################
load(con, "customers")
load(con, "employees")
load(con, "products")
load(con, "orders")
load(con, "order_details")

# Now queries can be sent to interrogate our system ############################
res <- query(con, "SELECT * FROM employees WHERE region = 'NULL'")
View(res)

# Or ask it to compute something
query(con, "SELECT count(companyName) AS count FROM customers")
query(con, "SELECT count(distinct companyName) AS count FROM customers")

# Thanks to relational algebra we can combine information from table and 
# denormalize the information
res <- query(con, "SELECT *
FROM employees e
INNER JOIN orders o ON o.employeeid = e.employeeid")
View(res)

# And ask for computation
res <- query(con, "SELECT e.firstname, count(orderid) as sales
FROM employees e
INNER JOIN orders o ON o.employeeid = e.employeeid
GROUP BY e.firstname
ORDER BY 2 DESC")
View(res)

# Transactions #################################################################
# Lets do a sale, we will create a function that does the following
# 1.- We should identify the customer
# 2.- Verify that the product exists
# 3.- Create a new order
# 4.- Associate products with orders
sale <- function(who, what, price, quantity, e_id){
  c_id <- query(con, paste0("SELECT customerid 
                                     FROM customers 
                                     WHERE companyname = '",who,"'"))
  
  # Let's see if there is product stock
  p_id <- query(con, paste0("SELECT productid 
                                     FROM products 
                                     WHERE productname = '",what,"' 
                                     AND unitsinstock > 0 
                                     AND discontinued = 0"))
  
  # We get the next order id
  o_id <- query(con, "SELECT MAX(orderid)+1 AS orderid FROM orders")
  
  # We insert a new order
  insert <- paste0("INSERT INTO orders(orderid, customerid, employeeid, orderdate) VALUES ("
                   ,o_id$orderid,",'"
                   ,c_id$customerid,"',"
                   ,e_id,","
                   ,"'2020-11-09 00:00:00'",")"
  )
  exec(con, insert)
  
  # Finally we do insert the order details to associate product to the order
  insert <- paste0("INSERT INTO order_details VALUES ("
                   ,o_id$orderid,","
                   ,p_id$productid,","
                   ,price,","
                   ,quantity,",0)"
  )
  exec(con, insert)
  
  return(o_id$orderid)
}

#  Customer : Frankenversand
#  Product : Geitost
#  Price : Product sale price
#  Quantity
#  Employee : who sold it
o_id <- sale("Frankenversand", "Geitost", 0.10, 1, 5)
query(con, paste0("SELECT * FROM order_details WHERE orderid = ",o_id))

# Clear the database
clearDB(con)

# Disconnect from the database
dbDisconnect(con)

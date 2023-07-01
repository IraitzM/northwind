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

# Inspect some relation
DBI::dbListFields(con, "customers")

# Insert data
exec(con, "INSERT INTO customers(customerid, companyname) VALUES (1, 'Deusto')")

# Read data
query(con, "SELECT * FROM customers")

# Update data
exec(con, "UPDATE customers SET contactname = 'Iraitz' WHERE companyname = 'Deusto'")

# Check constraints
exec(con, "INSERT INTO customers(customerid, companyname) VALUES (1, 'Deusto')")

# Erase a tuple
exec(con, "DELETE FROM customers WHERE companyname = 'Deusto'")
query(con, "SELECT * FROM customers")

# Lets get some data inside
load(con, "customers")
load(con, "employees")
load(con, "products")
load(con, "orders")
load(con, "order_details")

# Now queries can be sent to interrogate our system
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

# Using dplyr
employees <- tbl(con, "employees")
orders <- tbl(con, "orders")
by <- join_by(employeeid)
data <- inner_join(employees, orders)

employees %>% show_query()
orders %>% show_query()
data %>% show_query()
df <- data %>% collect()
View(df)

# And ask for computation
res <- query(con, "SELECT e.firstname, count(orderid) as sales
FROM employees e
INNER JOIN orders o ON o.employeeid = e.employeeid
GROUP BY e.firstname
ORDER BY 2 DESC")
View(res)

# Disconnect
dbDisconnect(con)

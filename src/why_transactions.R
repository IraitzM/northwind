# Let's explore why the need for transactions
# First we will need to import required libraries to interact with our local PostgreSQL RDBMS
renv::restore()

# Auxiliary functions
source('src/aux_functions.R')

# Software
# PostgreSQL

# Could be used Docker for this:
# 1. docker restart some-postgres
# 3. docker inspect some-postgres
dhost <- '172.17.0.2'

# Let's connect to our database
con <- DBI::dbConnect(RPostgres::Postgres(), 
                      user= 'postgres', 
                      password = 'postgres', 
                      host=dhost)

# List existing tables
list(con)

# Transactions
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
                                     AND unitsinstock > ", quantity," 
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
  
  # We take the quantity out of stock
  exec(con, paste0("UPDATE products SET unitsinstock = unitsinstock - ",quantity," WHERE productid = ", p_id))
  
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

# Check the stock
u_s <- query(con, "SELECT unitsinstock FROM products WHERE productname = 'Geitost'")$unitsinstock

# What happens if we go out of stock?
o_id <- sale("Frankenversand", "Geitost", 0.10, u_s+5, 5)
query(con, "SELECT * FROM products WHERE productname = 'Geitost'")

# Clear the database
clearDB(con)

# Disconnect
dbDisconnect(con)

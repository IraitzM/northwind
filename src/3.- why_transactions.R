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
                      password = 'mysecretpassword', 
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

# Better prepare a procedure for this
exec(con, 
     "CREATE OR REPLACE PROCEDURE sale(who text, what text, price float, quantity int, e_id int)
   LANGUAGE 'plpgsql'
   AS $$
DECLARE
       c_id text;
       p_id int;
       o_id int;
       units int;
BEGIN 

  SELECT customerid INTO c_id FROM customers WHERE companyname = $1;
  IF c_id IS NULL THEN 
    RAISE EXCEPTION 'Cannot find customer %', $1; 
  END IF; 
  
  SELECT productid INTO p_id 
    FROM products 
    WHERE productname = $2 
      AND discontinued = 0;
  IF p_id IS NULL THEN 
    RAISE EXCEPTION 'Cannot find product %', $2; 
  END IF; 
  
  SELECT MAX(orderid)+1 INTO o_id FROM orders;
  
  INSERT INTO orders(orderid, customerid, employeeid, orderdate) VALUES (o_id, c_id, e_id, '2020-11-09 00:00:00');
  
  UPDATE products SET unitsinstock = unitsinstock - $4 WHERE productid = p_id;
  
  INSERT INTO order_details VALUES (o_id,p_id,$3,$4,0);
  
  SELECT unitsinstock INTO units FROM products 
    WHERE productname = $2;
  
  IF units < 0 THEN
    RAISE NOTICE 'Not enough units in stock';
    ROLLBACK;
  END IF;
END;
$$;")

# Now we can see how the DB evolves
dbExecute(con, "CALL sale('Iraitz', 'Geitost', 0.10, 1, 5)")
dbExecute(con, "CALL sale('Frankenversand', 'Harddrive', 0.10, 1, 5)")
dbExecute(con, "CALL sale('Frankenversand', 'Geitost', 0.10, 10, 5)")
dbExecute(con, "CALL sale('Frankenversand', 'Geitost', 0.10, 1000, 5)")

# Disconnect
dbDisconnect(con)

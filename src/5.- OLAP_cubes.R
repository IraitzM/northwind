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

# Well, we know our queries run slow. Let's build some cube with aggregated information
# Dim#1 -> year
# Dim#2 -> month
# Dim#3 -> productid
# Dim#4 -> customerid
# Measure -> count(*)
cube <- query(con, "SELECT date_part('year', orderdate) AS year, date_part('month', orderdate) AS month, 
                                    customerid AS client, 
                                    productid AS product, 
                                    count(*) AS howmany
                            FROM orderline_fact
                            GROUP BY year, month, client, product")
storeTable(con, cube, "productivity")

# Now it should be faster to get our insights
# Two most productive months
query(con, "SELECT month, sum(howmany) 
                           FROM productivity 
                           GROUP BY month
                           ORDER BY 2 DESC
                           LIMIT 2")

# Three most productive months-client
query(con, "SELECT month, client, sum(howmany) 
                           FROM productivity 
                           GROUP BY month, client
                           ORDER BY 3 DESC
                           LIMIT 3")

# We could compute as many cubes as we want.
# Dim#1 -> yearmonth
# Dim#2 -> unitprice : low (< 5), medium (>= 5 AND < 30), high ( >= 30 )
# Dim#3 -> unitprice
# Dim#4 -> quantity
# Measure -> total
cube <- query(con, "SELECT date_part('year', orderdate)::TEXT || date_part('month', orderdate)::TEXT AS month, 
                                    CASE WHEN unitprice < 5 THEN 'low'
                                         WHEN unitprice >= 30 THEN 'high'
                                         ELSE 'medium' 
                                    END AS pricerange,
                                    unitprice AS price,
                                    quantity AS quantity, 
                                    (unitprice*quantity) AS total
                            FROM orderline_fact
                            GROUP BY month, price, quantity")
storeTable(con, cube, "salesprice")

# We could wrangle a little bit more or create some reports connected to our data repository. 
# Or just clear everything up
clearDB(con)
DBI::dbDisconnect(con)

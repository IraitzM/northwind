# Let's explore how to model our retail database
# First we will import required libraries to interact with our local PostgreSQL RDBMS

# Software
# PostgreSQL 10.2.5

# Libraries
renv::restore()
#
devtools::install_github("bergant/datamodelr")

# Auxiliary functions
source('src/aux_functions.R')

# Let's connect to our database
con <- DBI::dbConnect(RPostgres::Postgres(), user= 'postgres', password = 'postgres')

# List existing tables
list(con)

# Create relational model

# Show it


# Eliminamos las tablas
clearDB(con)

# Disconnect from the database
dbDisconnect(con)

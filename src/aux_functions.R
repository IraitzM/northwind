# Librerias
require(DBI)
require(dplyr)
require(arsenal)

# Following function erases the table if exists
# Parameters:
# -> conection : database conection
# -> name: table name

eraseIfExists <- function(con, name)
{
  if(dbExistsTable(con, name)){
    dbRemoveTable(con, name)
  }
}

# Clear database
# Parameters:
# -> conection : database conection
clearDB <- function(con){
  tList <- list(con)
  for (l in tList){
    eraseIfExists(con, l)
  }
}

# List existing tables
# Parameters:
# -> conection : database conection

list <- dbListTables

# Store table
# Parameters:
# -> conection : database conection
# -> dataframe : table data to be stored
# -> name : table name
storeTable <- function(con,dataframe, name){
  
  # TODO
}

# Send query
# Parameters:
# -> conection : database conection
# -> query : query to be sent
query <- function(con, query){
  dbFetch(dbSendQuery(con, query))
}

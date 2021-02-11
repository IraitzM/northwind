# Required libraries
require(DBI)
require(dplyr)
require(arsenal)
require(DiagrammeR)

# Following function erases the table if exists
# Parameters:
# -> connection : database connection
# -> name: table name

eraseIfExists <- function(con, name)
{
  if(dbExistsTable(con, name)){
    dbRemoveTable(con, name, cascade=TRUE)
  }
}

# Clear database
# Parameters:
# -> connection : database connection
clearDB <- function(con){
  exec(con, "DROP TABLE employees, products, orders, order_details, customers CASCADE;")
}

# List existing tables
# Parameters:
# -> connection : database connection
list <- dbListTables

# Store table
# Parameters:
# -> connection : database connection
# -> dataframe : table data to be stored
# -> name : table name
storeTable <- function(con,dataframe, name){
  
  eraseIfExists(con, name)
  dbWriteTable(conn = con, name = name, dataframe)
}

# Send query
# Parameters:
# -> connection : database connection
# -> query : query to be sent
query <- function(con, query){
  dbFetch(dbSendQuery(con, query))
}

# Exec command
# Parameters:
# -> connection : database connection
# -> query : query to be sent
exec <- function(con, query){
  res<-dbSendStatement(con, query)
  dbClearResult(res) 
}

# Reads data and writes it to a given table
# Parameters:
# -> connection : database connection
# -> fname : file name matching table name
load <- function(con, fname){
  df <- read.csv(paste0("data/", fname, ".csv"), as.is=T, sep = ',')
  colnames(df)<-tolower(colnames(df))
  dbAppendTable(con, fname, df)
}

# Show model
# Parameters:
# -> connection : database connection
#show <- function(con){
#  sQuery <- datamodelr::dm_re_query("postgres")
#  dm <- dbGetQuery(con, sQuery) 
#  dm <- datamodelr::as.data_model(dm)
#  graph <- datamodelr::dm_create_graph(dm, rankdir = "RL")
#  datamodelr::dm_render_graph(graph)
#}

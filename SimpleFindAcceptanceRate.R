library(RMySQL)
library(DBI)
library(chron)
rm(list=ls())

lapply(dbListConnections(dbDriver(drv="MySQL")), dbDisconnect)

myconn <- dbConnect(RMySQL::MySQL(), dbname="wingz-prod",host="wingz-platform-read001.c8voyumknq5z.us-west-1.rds.amazonaws.com",
                    username="wingz-read-only", password="4P4v53S256hW7Z2X")

rideRequestDf <- dbGetQuery(myconn, "SELECT * FROM rides 
                            WHERE state_id in (5,6) AND parent_id is null 
                            LIMIT 100")
acceptedRequestDF <- dbGetQuery(myconn, "SELECT * FROM rides 
                                WHERE parent_id is NOT null AND state_id in (6,7,8) 
                                LIMIT 100")
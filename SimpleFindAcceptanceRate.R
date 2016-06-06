library(RMySQL)
library(DBI)
library(chron)
library(data.table)
rm(list=ls()) #REMOVE ALL PREVIOUS ENVIRONMENT VARIABLES EVERYTIME SCRIPT IS RAN

lapply(dbListConnections(dbDriver(drv="MySQL")), dbDisconnect) #REMOVE ALL CONNECTIONS TO DATABASE TO AVOID OVERFLOW

myconn <- dbConnect(RMySQL::MySQL(), dbname="wingz-prod",host="wingz-platform-read001.c8voyumknq5z.us-west-1.rds.amazonaws.com",
                    username="wingz-read-only", password="4P4v53S256hW7Z2X")

rideRequestDf <- dbGetQuery(myconn, "SELECT id, date_reservation FROM rides 
                            WHERE state_id in (5,6) AND parent_id is null 
                            LIMIT 10000")
acceptedRequestDF <- dbGetQuery(myconn, "SELECT parent_id, date_reservation, date_accepted FROM rides 
                                WHERE parent_id is NOT null AND state_id in (6,7,8)
                                ORDER BY parent_id
                                LIMIT 10000")

rideRequestDf[,2] <- as.chron(rideRequestDf[,2]) #turn date_reservation column into chron objects for easier access
acceptedRequestDF[,2] <- as.chron(acceptedRequestDF[,2]) 

#GET THE ACCEPTED REQUESTS WITHIN THE DATE RANGE OF THE RIDE REQUESTS
acceptedRequestDF <- merge(x = acceptedRequestDF, y = rideRequestDf, by.x = "parent_id", by.y = "id")
#REMOVE DUPLICATES
duplicatedRows <- duplicated(acceptedRequestDF[,1])
acceptedRequestDF <- acceptedRequestDF[!duplicatedRows,]

#EXTRACT EACH HOURS REQUESTS
RequestsEachHour <- vector(mode = "list")
for(i in 1:24){
  indices <- which(hours(rideRequestDf$date_reservation) == i-1)
  RequestsEachHour[i] <- length(indices)
}

#EXTRACT EACH HOURS ACCEPTED REQUESTS
acceptedRequestsEachHour <- vector(mode = "list") #EXTRACT EACH HOUR'S ACCEPTED COUNT 
for(i in 1:24){
  indices <- which(hours(acceptedRequestDF$date_reservation.y) == i-1)
  acceptedRequestsEachHour[i] <- length(indices)
}

#CALCULATE PROBABILITIES FOR EACH HOUR AND STORE IN A VECTOR
vectorOfProbabilites <- vector(mode = "list")
for(i in 1:24){
  vectorOfProbabilites[i] <- as.numeric(acceptedRequestsEachHour[i])/as.numeric(RequestsEachHour[i])
}


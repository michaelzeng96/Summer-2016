library(DBI)
library(chron)
library(RMySQL)
rm(list=ls())
lapply(dbListConnections(dbDriver(drv="MySQL")), dbDisconnect)

myconn <- dbConnect(RMySQL::MySQL(), dbname="wingz-prod",host="wingz-platform-read001.c8voyumknq5z.us-west-1.rds.amazonaws.com",
                    username="wingz-read-only", password="4P4v53S256hW7Z2X")
#rideRequestDf <- dbGetQuery(myconn, "SELECT * FROM rides WHERE state_id in (5,6) AND parent_id is null LIMIT 100")
#acceptedRequestDF <- dbGetQuery(myconn, "SELECT * FROM rides WHERE parent_id is NOT null AND state_id in (6,7,8) LIMIT 100")

acceptanceRateDF <- dbGetQuery(myconn, "SELECT
  ride_requests.id AS ride_requests_id,
                               ride_requests.airport,
                               ride_requests.date_reservation,
                               gross_accepted.date_accepted,
                               gross_accepted.gross_accepted_flag
                               FROM rides AS ride_requests
                               LEFT JOIN
                               (SELECT
                               gross_accepted.parent_id,
                               min(gross_accepted.date_accepted) AS date_accepted,
                               1                                 AS gross_accepted_flag
                               FROM rides AS gross_accepted
                               WHERE gross_accepted.state_id IN (6, 7, 8) AND gross_accepted.parent_id IS NOT NULL
                               GROUP BY 1) AS gross_accepted ON ride_requests.id = gross_accepted.parent_id LIMIT 1000")

acceptanceRateDF[,3] <- as.chron(acceptanceRateDF[,3]) #CONVERT date_reservation into chron object
acceptanceRateDF[,4] <- as.chron(acceptanceRateDF[, 4]) #CONVERT date_accepted into chron object

acceptanceRateDF[is.na(acceptanceRateDF)] <- 0 #REPLACE ALL NA'S WITH 0'S

hoursList <- vector("list", 24)
for(i in 1:24){
  hoursList[i] <- length(which(hours(acceptanceRateDF$date_accepted) == i-1))
}
hoursList <- as.numeric(hoursList)

countNumOfAccepted <- function(hour){ #COUNT NUMBER OF ACCEPTED REQUESTS OUT OF ALL REQUESTS FOR SPECIFIED HOUR
  counter <- 0
  indices <- which(hours(acceptanceRateDF$date_accepted) == hour)
  for(i in 1:length(indices)){
    if(acceptanceRateDF$gross_accepted_flag[indices[i]] == 1){
        counter <- counter + 1
    }
  }
  return(counter)
}

countNumOfDeclined <- function(hour){ #COUNT NUMBER OF DECLINED REQUESTS OUT OF ALL REQUESTS FOR SPECIFIED HOUR
  counter <- 0
  indices <- which(hours(acceptanceRateDF$date_accepted) == hour)
  for(i in 1:length(indices)){
    if(acceptanceRateDF$gross_accepted_flag[indices[i]] == 0){
      counter <- counter + 1
    }
  }
  return(counter)
}

acceptedHoursList <- vector("list", 24)
for(i in 1:24){
  acceptedHoursList[i] <- countNumOfAccepted(i - 1) 
}

declinedHoursList <- vector("list", 24)
for(i in 1:24){
  declinedHoursList[i] <- countNumOfDeclined(i - 1)
}


acceptedDF <- data.frame()

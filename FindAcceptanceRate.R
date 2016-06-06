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

#acceptanceRateDF[is.na(acceptanceRateDF)] <- 0 #REPLACE ALL NA'S WITH 0'S

hoursList <- vector("list", 24)
for(i in 1:24){
  hoursList[i] <- length(which(hours(acceptanceRateDF$date_reservation) == i-1)) #HOW MANY REQUESTS RECEIVED IN EACH HOUR
}
hoursList <- as.numeric(hoursList) #24 HOURS, EACH HOUR HAS ITS OWN TOTAL NUM OF NOTIFICATIONS

countNumOfAccepted <- function(hour){ #COUNT NUMBER OF ACCEPTED REQUESTS OUT OF ALL REQUESTS FOR SPECIFIED HOUR
  counter <- 0
  indices <- which(hours(acceptanceRateDF$date_reservation) == hour)
  for(i in indices){
    if(is.na(acceptanceRateDF$gross_accepted_flag[i]))
      next
    if(acceptanceRateDF$gross_accepted_flag[i] == 1){
        counter <- counter + 1
        #print(i)
    }
  }
  return(counter)
}

countNumOfDeclined <- function(hour){ #COUNT NUMBER OF DECLINED REQUESTS OUT OF ALL REQUESTS FOR SPECIFIED HOUR
  counter <- 0
  indices <- which(hours(acceptanceRateDF$date_reservation) == hour)
  for(i in indices){
    if(is.na(acceptanceRateDF$gross_accepted_flag[i])){
      counter <- counter + 1
      #print(i)
    }
  }
  return(counter)
}


acceptedHoursList <- vector("list", 24)
for(i in 1:24){
  acceptedHoursList[i] <- countNumOfAccepted(i - 1) #CREATES VECTOR OF 24 HOURS, EACH HOUR HAS A NUMBER OF ACCEPTED NOTIFICATIONS 
}
acceptedHoursList <- as.numeric(acceptedHoursList)

declinedHoursList <- vector("list", 24) #CREATES VECTOR OF 24 HOURS, EACH HOUR HAS A NUMBER OF DECLIED. 
for(i in 1:24){                         #MORE FOR TESTING/VERIFICATION PURPOSES, TO MAKE SURE FUNCTIONS ARE CORRECT
  declinedHoursList[i] <- countNumOfDeclined(i - 1)
}
declinedHoursList <- as.numeric(declinedHoursList)

findAcceptanceRate <- function(hour){ #CALCULATE HOW MANY NOTIFICATIONS WERE ACCEPTED ON THIS HOUR. ACCEPTED/TOTAL 
  probability <- acceptedHoursList[hour]/hoursList[hour]
  return(probability)
}

acceptanceRatePerHour <- vector("list", 24)
for(i in 1:24){
  acceptanceRatePerHour[i] <- findAcceptanceRate(i) #VECTOR OF 24 HOURS, EACH HOUR HAS A ACCEPTANCE RATE 
}





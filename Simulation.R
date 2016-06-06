library(DBI)
library(chron)
library(parallel)
library(RMySQL)
rm(list=ls()) #clear Global Environment with each run of script
lapply(dbListConnections(dbDriver(drv="MySQL")), dbDisconnect)



#ordered by when the request arrived 
myconn <- dbConnect(RMySQL::MySQL(),dbname="wingz-prod",host="wingz-platform-read001.c8voyumknq5z.us-west-1.rds.amazonaws.com",
                    username="wingz-read-only", password="4P4v53S256hW7Z2X")
rideRequestDf <- dbGetQuery(myconn, "SELECT date_reservation, departure_date_utc FROM rides LIMIT 1000") #NUMBER OF RIDES
rideRequestDf[,1] <- as.chron(rideRequestDf[,1])
rideRequestDf[,2] <- as.chron(rideRequestDf[,2])

rideRequests <- rideRequestDf[,1,drop=FALSE] #all requests 
departureDates <- rideRequestDf[,2,drop=FALSE] #departure dates
departureDates$hour <- 0 #add hour component to each departureDate
departureDates$counter <- 1 #add counter component to each departureDate. init to 0

rideRequestDf1 <- dbGetQuery(myconn, "SELECT id, date_reservation FROM rides 
                            WHERE state_id in (5,6) AND parent_id is null 
                             LIMIT 10000")
acceptedRequestDF1 <- dbGetQuery(myconn, "SELECT parent_id, date_reservation, date_accepted FROM rides 
                                 WHERE parent_id is NOT null AND state_id in (6,7,8)
                                 ORDER BY parent_id
                                 LIMIT 10000")

rideRequestDf1[,2] <- as.chron(rideRequestDf1[,2]) #turn date_reservation column into chron objects for easier access
acceptedRequestDF1[,2] <- as.chron(acceptedRequestDF1[,2]) 

#GET THE ACCEPTED REQUESTS WITHIN THE DATE RANGE OF THE RIDE REQUESTS
acceptedRequestDF1 <- merge(x = acceptedRequestDF1, y = rideRequestDf1, by.x = "parent_id", by.y = "id")
#REMOVE DUPLICATES
duplicatedRows <- duplicated(acceptedRequestDF1[,1])
acceptedRequestDF1 <- acceptedRequestDF1[!duplicatedRows,]

#EXTRACT EACH HOURS REQUESTS
RequestsEachHour <- vector(mode = "list")
for(i in 1:24){
  indices <- which(hours(rideRequestDf1$date_reservation) == i-1)
  RequestsEachHour[i] <- length(indices)
}

#EXTRACT EACH HOURS ACCEPTED REQUESTS
acceptedRequestsEachHour <- vector(mode = "list") #EXTRACT EACH HOUR'S ACCEPTED COUNT 
for(i in 1:24){
  indices <- which(hours(acceptedRequestDF1$date_reservation.y) == i-1)
  acceptedRequestsEachHour[i] <- length(indices)
}

#CALCULATE PROBABILITIES FOR EACH HOUR AND STORE IN A VECTOR
vectorOfProbabilites <- vector(mode = "list")
for(i in 1:24){
  vectorOfProbabilites[i] <- as.numeric(acceptedRequestsEachHour[i])/as.numeric(RequestsEachHour[i])
}


#Number of Drivers
NumOfDriver <- 50

#Array of driver IDs
DriverIDs <- c(1:50)

#24 Hours in a day
HoursOfDay <- 24

AcceptedRides <- 0
ExpiredRides <- 0

#Random values 1 and 0, 1 = Can Take, 0 = Cannot Take
ableOrNotAble <- sample(0:1, HoursOfDay*NumOfDriver, replace=T)

#Capacity Matrix
capacityMat <- matrix(ableOrNotAble, nrow = NumOfDriver, ncol = 24, byrow = T)

#Pool of accepted requests, [(DriverID, Accepted request date)]
acceptedRequests <- data.frame(dID = numeric(), h = numeric()) #init dataframe

canRead <- function(hour){ #use a random uniform distribution i.e 0.2 <= driver probability? 
  #x <- runif(1,0.0,1.0)
  #if(x <= driverProbability)
  #  return(TRUE)
  #return(FALSE)
  prob <- vectorOfProbabilites[hour+1]
  #print(prob)
  x <- runif(1, 0.0, 1.0)
  if(x <= prob)
    return(TRUE)
  return(FALSE)
}

extractHour <- function(date){ #return int from chron date
  return(hours(date))
}

for(i in 1:length(departureDates[[1]])){
  departureDates$hour[i] <- extractHour(departureDates$departure_date_utc[i]) #ADD HOURS TO EACH DEPARTURE DATE
}

reSampleDrivers <- function(excludeDriverID){
  
}

isRequestNotInPool <- function(driverID, hour){ #return true if there is NO match
  existingDriverIndex <- which(acceptedRequests$dID == driverID)
  for (i in existingDriverIndex){
    if(acceptedRequests$h[i] == hour)
      return(FALSE)
  }
  return (TRUE)
}

checkCapcityMatrix <- function(driverID, hour){
  if(capacityMat[driverID, hour+1] == 1)
    return(TRUE)
  else
    return(FALSE)
}

populateAcceptedPool <- function(driverID, hour){
  acceptedRequests[nrow(acceptedRequests)+1, ]<<-c(driverID,hour)
}

simulate <- function(){
  for(i in 1:length(departureDates[[1]])){
    #print(departureDates$departure_date[i])##############
    randDriverVector <- sample(1:50,50) #create vector of random drivers, iterate through each one until request expires
    for(selectedDriver in randDriverVector){
      if(departureDates$counter[i] >= 5){ #IF THE REQUEST GOES 5 COUNTS W/O BEING ACCEPTED, COUNT IT AS EXPIRED
        #print("Expired")################
        #print(ExpiredRides)
        ExpiredRides <<- ExpiredRides + 1
        #print(paste0("Second time=",ExpiredRides))
        #Stop("stop")
        break
      }
      x <- canRead(departureDates$hour[i]) #checks if this driver can read notification
      #print(paste(x," canRead?"))#############################
      if(x == TRUE){
        is_Free <- isRequestNotInPool(selectedDriver, departureDates$hour[i]) #check AcceptedRequest data frame to see if driver has already been booked for this time
        #print(paste(is_Free, " isFreeDuringThisTime?"))###########################
        if(is_Free == TRUE){
          canAccept <- checkCapcityMatrix(selectedDriver, departureDates$hour[i]) #check if driver can take the ride him/her-self
          #print(paste(canAccept," has capacity to accept??"))########################
          if(canAccept == TRUE){
            populateAcceptedPool(selectedDriver, departureDates$hour[i]) #populate accepted drivers vector and increment both accepted counter and request counter 
            AcceptedRides <<- AcceptedRides + 1
            break
          }
          else{ #if driver CANNOT take the ride him/her-self
            departureDates$counter[i] <<- departureDates$counter[i] + 1
            next #move to next driver and increment the request's count
            }
        }
        else{ #if driver IS NOT FREE, and is already booked by the request pool 
          departureDates$counter[i] <<- departureDates$counter[i] + 1
          next #move to next driver and increment the request's count
        }
      }
      #if driver IGNORES notification
      else{
        departureDates$counter[i] <<- departureDates$counter[i] + 1
        next #move to next driver and increment the request's count
      }
    }
  }
  show() 
}

show <- function(){
  #STATISTICS BELOW AND HISTOGRAM 
  hist(departureDates$counter, freq=FALSE, breaks=c(1,2,3,4,5))
  result.mean <- mean(departureDates$counter)
  result.median <- median(departureDates$counter)
  #result.mode <- mode(departureDates$counter)
  print(paste("Mean is: ",result.mean))
  print(paste("Median is: ",result.median))
  print(paste("There were: ",AcceptedRides, " accepted rides."))
  print(paste("There were: ",ExpiredRides, " expired rides."))

}

simulate()



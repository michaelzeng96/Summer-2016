library(DBI)
library(chron)
library(parallel)
library(RMySQL)
rm(list=ls()) #clear Global Environment with each run of script
lapply(dbListConnections(dbDriver(drv="MySQL")), dbDisconnect)


#SET UP SQL CONNECTION
myconn <- dbConnect(RMySQL::MySQL(),dbname="wingz-prod",host="wingz-platform-read001.c8voyumknq5z.us-west-1.rds.amazonaws.com",
                    username="wingz-read-only", password="4P4v53S256hW7Z2X")


#SET NUMBER OF RIDE REQUESTS WITH SQL "LIMIT"
rideRequestDf <- dbGetQuery(myconn, "SELECT date_reservation, departure_date_utc FROM rides LIMIT 1000") 

#CONVERT BOTH COLUMNS OF RIDEREQUEST TO CHRON OBJECTS FOR EASIER ACCESS TO THE DATES
rideRequestDf[,1] <- as.chron(rideRequestDf[,1])  
rideRequestDf[,2] <- as.chron(rideRequestDf[,2])

#CREATE NEW DFs TO SEPEARTE REQUESTS AND DEPARTURE DATES, rideRequests AND departureDates 
rideRequests <- rideRequestDf[,1,drop=FALSE] 
departureDates <- rideRequestDf[,2,drop=FALSE] 

#ADD HOUR COMPONENT FOR EASIER ACCESS TO HOUR
#ADD COUNTER COMPONENT TO KEEP TRACK OF HOW MANY TIMES EACH REQUEST NOTIFICATION IS SENT
departureDates$hour <- 0 #add hour component to each departureDate
departureDates$counter <- 1 #add counter component to each departureDate. init to 0

#######################################################################################################################
####################################### CODE TOCALCULATE DRIVER/HOUR PROBABILITIES ###################################
#######################################################################################################################

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

#######################################################################################################################
###################################################END OF CODE########################################################
#######################################################################################################################


#DRIVER INFORMATION. number of drivers, corresponding IDs
NumOfDriver <- 50
DriverIDs <- c(1:50)

#24 HOURS IN A DAY
HoursOfDay <- 24

#VARIABLES TO KEEP TRACK OF TOTAL accepted rides AND expired rides
AcceptedRides <- 0
ExpiredRides <- 0

#RANDOM VALUES FOR CAPACITY MATRIX. 24 BY 50 = 1200 VALUES. 1 = Can Take, 0 = Cannot Take
ableOrNotAble <- sample(0:1, HoursOfDay*NumOfDriver, replace=T)

#CAPACITY MATRIX. ARBITRARY BINARY VALUES WHICH TELL IF A SPECIFIC DRIVER CAN/CANNOT TAKE A RIDE AT A CERTAIN HOUR
capacityMat <- matrix(ableOrNotAble, nrow = NumOfDriver, ncol = 24, byrow = T)






#DATAFRAME OF ALL ACCEPTED REQUESTS. NEEDED TO DETERMINE IF A DRIVER IS BUSY AT A CERTAIN TIME 
#  DriverID, Hour Busy
acceptedRequests <- data.frame(dID = numeric(), h = numeric()) 

#FUNCTION TO POPULATE acceptedRequests DATAFRAME WITH ACCEPTED REQUEST AND ITS DRIVER
populateAcceptedPool <- function(driverID, hour){
  acceptedRequests[nrow(acceptedRequests)+1, ]<<-c(driverID,hour)
}



#FUNCTION TO GET THE HOUR OF A CHRON DATE
extractHour <- function(date){ 
  return(hours(date))
}

#POPULATE HOUR COLUMN IN departureDates DATAFRAME, FOR EASIER/DIRECT ACCESS TO THE NEEDED HOUR
for(i in 1:length(departureDates[[1]])){
  departureDates$hour[i] <- extractHour(departureDates$departure_date_utc[i]) 
}




#FUNCTION TO SEE IF DRIVER WILL READ NOTIFICATION BASED ON PROBABILITY VECTOR
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



#FUNCTION TO CHECK THE CAPACITY MATRIX TO SEE IF DRIVER CAN TAKE A RIDE OR NOT
checkCapcityMatrix <- function(driverID, hour){
  if(capacityMat[driverID, hour+1] == 1)
    return(TRUE)
  else
    return(FALSE)
}



#CHECK ALL ACCUMULATED ACCEPTED REQUESTS BY DRIVER, TO SEE IF A REQUEST'S HOUR IS ALREADY BOOKED. 
isRequestNotInPool <- function(driverID, hour){ #return true if there is NO match
  existingDriverIndex <- which(acceptedRequests$dID == driverID) #checks all taken hours by driver
  for (i in existingDriverIndex){
    if(acceptedRequests$h[i] == hour) #if driver already is booked during this hour, return false
      return(FALSE)
  }
  return (TRUE)
}






########################################## FUNCTION TO SIMULATE THE SYSTEM ###########################################

#NUMBER OF MINUTES AFTER INITIAL DRIVER NOTIFICATION BEFORE EXPIRATION (init to 0)
minutesBeforeExpiration <- 6


simulate <- function(){
  for(i in 1:length(departureDates[[1]])){
    randDriverOrder <- sample(1:50,50) #random selection of iteration of drivers per simulation
    for(selectedDriver in randDriverOrder){
      #IF DRIVER IS ALREADY TAKEN, MOVE ONTO NEXT DRIVER. CHECKS IF REQUEST HAS ALREADY EXPIRED
      if(departureDates$counter[i] >= minutesBeforeExpiration){  
        ExpiredRides <<- ExpiredRides + 1 
        break
      }
      #CHECK IF DRIVER CAN READ NOTIFICATION. IF CANNOT, MOVE ONTO NEXT DRIVER 
      if(canRead(departureDates$hour[i]) == TRUE){
        #CHECK IF DRIVER IS ALREADY BOOKED DURING THIS REQUEST'S HOUR. IF IS BOOKED, MOVE ONTO NEXT DRIVER 
        if(isRequestNotInPool(selectedDriver, departureDates$hour[i]) == TRUE){
          #CHECK IF DRIVER HAS CAPACITY TO TAKE THIS RIDE. IF NO CAPACITY, MOVE ONTO NEXT DRIVER
          #IF REQUEST PASS ALL CONDITIONS, POPULATE ACCEPTED POOL AND INCREMENT ACCEPTED RIDES
          if(checkCapcityMatrix(selectedDriver, departureDates$hour[i]) == TRUE){
            populateAcceptedPool(selectedDriver, departureDates$hour[i]) 
            AcceptedRides <<- AcceptedRides + 1
            break
          }
          else{ #if driver CANNOT take the ride due to CAPACITY MATRIX
            departureDates$counter[i] <<- departureDates$counter[i] + 1
            next #move to next driver and increment the request's count
            }
        }
        else{ #if driver IS NOT FREE, and is already booked accoringt to the request pool 
          departureDates$counter[i] <<- departureDates$counter[i] + 1
          next #move to next driver and increment the request's count
        }
      }
      else{  #if driver DOES NOT READ notification
        departureDates$counter[i] <<- departureDates$counter[i] + 1
        next #move to next driver and increment the request's count
      }
    }
  }
  show() 
}

show <- function(){
  #STATISTICS BELOW AND HISTOGRAM 
  hist(departureDates$counter, freq=FALSE, breaks=c(1,2,3,4,5,6))
  result.mean <- mean(departureDates$counter)
  result.median <- median(departureDates$counter)
  #result.mode <- mode(departureDates$counter)
  print(paste("Mean is: ",result.mean))
  print(paste("Median is: ",result.median))
  print(paste("There were: ",AcceptedRides, " accepted rides."))
  print(paste("There were: ",ExpiredRides, " expired rides."))

}

simulate()



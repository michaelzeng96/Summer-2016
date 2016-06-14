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
RequestsDF <- dbGetQuery(myconn, "SELECT date_reservation, departure_date_utc FROM rides WHERE date_reservation > '2016-06-01' LIMIT 1000") 

#CONVERT BOTH COLUMNS OF RIDEREQUEST TO CHRON OBJECTS FOR EASIER ACCESS TO THE DATES
RequestsDF[,1] <- as.chron(RequestsDF[,1])  
RequestsDF[,2] <- as.chron(RequestsDF[,2])


#ADD HOUR COMPONENT FOR EASIER ACCESS TO HOUR
#ADD COUNTER COMPONENT TO KEEP TRACK OF HOW MANY TIMES EACH REQUEST NOTIFICATION IS SENT
#ADD LEADTIME COMPONENT TO KEEP TRACK OF HOW MUCH TIME THERE IS BETWEEN INITIAL REQUEST AND DEPARTURE TIME
RequestsDF$ReservedHour <- 0
RequestsDF$DepartureHour <- 0
RequestsDF$Counter <- 0
RequestsDF$LeadTime <- 0


#######################################################################################################################
####################################### CODE TO CALCULATE DRIVER/HOUR PROBABILITIES ###################################
#######################################################################################################################

RequestsDF1 <- dbGetQuery(myconn, "SELECT id, date_reservation, airport FROM rides 
                            WHERE state_id in (5,6) AND parent_id is null AND airport='SEA' 
                             LIMIT 10000")
acceptedRequestDF1 <- dbGetQuery(myconn, "SELECT parent_id, date_reservation, date_accepted, airport FROM rides 
                                 WHERE parent_id is NOT null AND state_id in (6,7,8) AND airport='SEA'
                                 ORDER BY parent_id
                                 LIMIT 10000")

RequestsDF1[,2] <- as.chron(RequestsDF1[,2]) #turn date_reservation column into chron objects for easier access
acceptedRequestDF1[,2] <- as.chron(acceptedRequestDF1[,2]) 

#GET THE ACCEPTED REQUESTS WITHIN THE DATE RANGE OF THE RIDE REQUESTS
acceptedRequestDF1 <- merge(x = acceptedRequestDF1, y = RequestsDF1, by.x = "parent_id", by.y = "id")
#REMOVE DUPLICATES
duplicatedRows <- duplicated(acceptedRequestDF1[,1])
acceptedRequestDF1 <- acceptedRequestDF1[!duplicatedRows,]

#EXTRACT EACH HOURS REQUESTS
RequestsEachHour <- vector(mode = "list")
for(i in 1:24){
  indices <- which(hours(RequestsDF1$date_reservation) == i-1)
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

#REMOVE UNNECESSARY VARIABLES EXCEPT FOR vectorOfProbabilities WHICH WILL BE USED
rm(acceptedRequestDF1,RequestsDF1,duplicatedRows,RequestsEachHour,indices,acceptedRequestsEachHour)

#######################################################################################################################
###################################################END OF CODE########################################################
#######################################################################################################################


##################################DRIVER DATAFRAME TO KEEP DRIVER INFORMATION########################################## 
NumOfDriver <- 250
DriverDF <- data.frame(matrix(nrow = NumOfDriver))
colnames(DriverDF) <- "driverID"

#FUNCTION TO CREATE AND PRIORITIZE A POOL OF DRIVERS
prioritizeDrivers <- function(NumOfDrivers){
  driverList <- sample(1:NumOfDrivers, NumOfDrivers)
  DriverDF$driverID <<- driverList
  return(driverList)
}



##############################################END OF DRIVER INFORMATION#################################################
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

#POPULATE RESERVATION HOUR COLUMN IN DATAFRAME, FOR EASIER/DIRECT ACCESS TO THE NEEDED HOUR
for(i in 1:length(RequestsDF[[1]])){
  RequestsDF$ReservedHour[i] <- extractHour(RequestsDF$date_reservation[i]) 
}

#POPULATE DEPARTURE HOUR COLUMN IN DATAFRAME, FOR EASIER/DIRECT ACCESS TO THE NEEDED HOUR
for(i in 1:length(RequestsDF[[1]])){
  RequestsDF$DepartureHour[i] <- extractHour(RequestsDF$departure_date_utc[i]) 
}

#CALCULATE LEAD TIME FOR EVERY REQUEST, FOR EASIER/DIRECT ACCESS TO THE LEAD TIME UNIT IS HOURS
for(i in 1:length(RequestsDF[[1]])){
  RequestsDF$LeadTime[i] <- round(difftime(RequestsDF$departure_date_utc[i], RequestsDF$date_reservation[i], units = "hours"))*60
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


#INFORMATION REGARDING GENERAL DISTRIBUTION MODEL ALGORITHM
numOfDriversInPool <- NumOfDriver #how many drivers are available
LastNotificationResponseAllowance <- 5 #time after last driver is notified before going into triage/leaving the system
blastSizeCoefficient <- 1 #controls blast size. 1=start small grow fast, 100=start with many drivers grow slow

#FUNCTION TO GENERATE HOW MUCH TIME IS AVAILABLE FOR DISTRIBUTION (ONLY GENERAL, IMPLEMENT TEAM DISTRO LATER)
#L = lead time, E = ride expiration time, T = team distribution time, G = general distribution time, M = idle time
#C = riders contigency time (once system stops worrying about distribution)
avaiableTime <- function(L){
  E <- round( 5+max(0,L-15)/15 + max(0,L-30)/15 + max(0,L-45)/15 + max(0,L-60)/15 + max(0,L-75)/15 + max(0,L-165)/15*10 - max(0,L-240)/15*13.5 + max(0,L-1440)/15 - max(0,L-4320)/15*2.5)
  M <- round(max(0,min(L-210,E/2)))
  T <- round( 2*(E-M)/5 )
  G <- round( 3*(E-M)/5 )
  C <- L-E
  G <- G+T
  T=0
  return(E)
}


#FUNCTION TO NOTIFY ONE DRIVER
notifyDriver <- function(driverID, RequestsDF){
  #CHECK IF DRIVER CAN READ NOTIFICATION. IF CANNOT, MOVE ONTO NEXT DRIVER 
  if(canRead(RequestsDF$DepartureHour[i]) == TRUE){
    print("Can read")
    #CHECK IF DRIVER IS ALREADY BOOKED DURING THIS REQUEST'S HOUR. IF IS BOOKED, MOVE ONTO NEXT DRIVER 
    if(isRequestNotInPool(driverID, RequestsDF$DepartureHour[i]) == TRUE){
      #CHECK IF DRIVER HAS CAPACITY TO TAKE THIS RIDE. IF NO CAPACITY, MOVE ONTO NEXT DRIVER
      #IF REQUEST PASS ALL CONDITIONS, POPULATE ACCEPTED POOL AND INCREMENT ACCEPTED RIDES
      print("is not in pool")
      if(checkCapcityMatrix(driverID, RequestsDF$DepartureHour[i]) == TRUE){
        print("has capacity")
        populateAcceptedPool(driverID, RequestsDF$DepartureHour[i]) 
        AcceptedRides <<- AcceptedRides + 1
        RequestsDF$Counter[i] <<- RequestsDF$Counter[i] + 1
        return()
      }
      else{ #if driver CANNOT take the ride due to CAPACITY MATRIX
        RequestsDF$Counter[i] <<- RequestsDF$Counter[i] + 1
        return() #move to next driver and increment the request's count
      }
    }
    else{ #if driver IS NOT FREE, and is already booked accoringt to the request pool 
      RequestsDF$Counter[i] <<- RequestsDF$Counter[i] + 1
      return() #move to next driver and increment the request's count
    }
  }
  else{  #if driver DOES NOT READ notification
    RequestsDF$Counter[i] <<- RequestsDF$Counter[i] + 1
    return() #move to next driver and increment the request's count
  }
}

#FUNCTION TO RUN THROUGH A LIST OF DRIVERS
notifyDrivers <- function(leftIndex, rightIndex, DriversDF, RequestsDF){
  listOfDrivers <- as.list(DriverDF[c(leftIndex:rightIndex),])
  for(i in 1:length(listOfDrivers)){
    print(paste0("notified ", DriverDF$driverID[i]))
    timeLeft <- rideExpiration - LastNotificationResponseAllowance
    print(paste0("Minutes left: ",timeLeft))
    if(RequestsDF$Counter[i] > timeLeft){
      print("Expired.")
      ExpiredRides <<- ExpiredRides + 1
    }
    notifyDriver(DriverDF$driverID[i] ,RequestsDF)
  }
}



simulate <- function(){
  for(i in 1:8){
    print(paste0("Leadtime is: ", RequestsDF$LeadTime[i]))
    if(RequestsDF$LeadTime[i] > 240){
      next
      }
    #calculate log base
    logBase <- (factorial(avaiableTime(RequestsDF$LeadTime[i])-LastNotificationResponseAllowance)*blastSizeCoefficient^(avaiableTime(RequestsDF$LeadTime[i])-LastNotificationResponseAllowance))^(1/(numOfDriversInPool+3))
    print(paste0("log base of: ",logBase))
    #calculate ride expiration
    rideExpiration <<- avaiableTime(RequestsDF$LeadTime[i])
    print(paste0("ride expires in: ", rideExpiration))
    #step1) create pool of drivers in sorted order. put them in a queue (RAND FOR NOW)
    driverList <- prioritizeDrivers(NumOfDriver)
    #step2) LOOP, calculate N number of drivers to notify. 
    #step3) in the same loop, create the list of drivers by selecting N drivers from front of queue
    #step4) run notifyDrivers() on that list
    dListIndexLeft <- 1
    dListIndexRight <- 1
    totalNotifiedDrivers <<- 0
    while(totalNotifiedDrivers < numOfDriversInPool){
      #N chooses number of drivers to notify per minute/blast
      N <-min(round(max(log(blastSizeCoefficient*RequestsDF$Counter[i], logBase),1),0),numOfDriversInPool-totalNotifiedDrivers)
      print(paste0("blast size: ", N))
      totalNotifiedDrivers <<- totalNotifiedDrivers + N
      dListIndexRight <<- dListIndexRight + N
      notifyDrivers(dListIndexLeft,dListIndexRight,DriverDF,RequestsDF)
      dListIndexLeft <<- dListIndexRight
      print(paste0("total notified drivers: ", totalNotifiedDrivers))
    }
  }
  #show() 
}

show <- function(){
  #STATISTICS BELOW AND HISTOGRAM 
  hist(departureDates$counter, freq=FALSE, breaks=c(1,2,3,4,5,6))
  result.mean <- mean(departureDates$counter)
  result.median <- median(departureDates$counter)
  #result.mode <- mode(departureDates$counter)
  print(paste0("Mean is: ",result.mean))
  print(paste0("Median is: ",result.median))
  print(paste0("There were: ",AcceptedRides, " accepted rides."))
  print(paste0("There were: ",ExpiredRides, " expired rides."))

}

simulate()


#ask alex balva about bound on lead time. and what ride expiration is given to it if it exceeds the bound
library(DBI)
library(RMySQL)
library(xlsx)
#CLEAR ENVIRONMENT AND DISCONNECT PREVIOUS CONNECTIONS EACH TIME SCRIPT IS RAN
rm(list=ls())
lapply(dbListConnections(dbDriver(drv="MySQL")),dbDisconnect)

#CONNECTION TO THE DATABASE
myconn <- dbConnect(RMySQL::MySQL(),dbname="wingz-prod",host="wingz-platform-read001.c8voyumknq5z.us-west-1.rds.amazonaws.com",
                    username="wingz-read-only", password="4P4v53S256hW7Z2X")

#READ IN ALL SURVEY RESPONSES
responsesDF <- read.csv("Responses.csv", header=TRUE)

#QUERY TO RETRIEVE DATABASE OF DRIVER/ZONE INFORMATION
realDatabaseDF <- dbGetQuery(myconn, "SELECT * FROM utilisateurs_has_zones LIMIT 100")
emailsDF <- dbGetQuery(myconn, "SELECT id_utilisateur, email FROM utilisateurs " )

#GRAB ALL EMAILS
allEmails <- data.frame(responsesDF$Wingz.Email.Address)
#REMOVE DUPLICATES
allEmails <- subset(allEmails, !duplicated(allEmails))
responsesDF <- subset(responsesDF, !duplicated(responsesDF$Wingz.Email.Address))

#PAIR ALL EMAILS WITH AN ID, THEN PUT INTO DATAFRAME
indices <- c(1,3,4,5)
for(i in 1:nrow(allEmails)){
  if(!length(which(tolower(emailsDF$email) == tolower(allEmails$responsesDF.Wingz.Email.Address[i])))){
    indices[i] <- NA
    next
  }
  indices[i] <- which(tolower(emailsDF$email) == tolower(allEmails$responsesDF.Wingz.Email.Address[i]))
}

IDandEmailDF <- data.frame()
for(i in 1:length(indices)){
  IDandEmailDF[nrow(IDandEmailDF)+1, "id_utilisateur"] <- emailsDF$id_utilisateur[indices[i]]
  IDandEmailDF[nrow(IDandEmailDF), "email_utilisateur"] <- allEmails$responsesDF.Wingz.Email.Address[i]
}

getDriverID <- function(entry){
  indice <- which(IDandEmailDF$email_utilisateur == entry)
  return(IDandEmailDF$id_utilisateur[indice])
}

weightFunction <- function(entry){
  if(entry == "Preferred")
    return(1)
  else if(entry == "Neutral")
    return(2)
  else if(entry == "Rejected")
    return(3)
}

#listOfZones <- c("inner town", "outer town","northeast", "northwest", "west zone", "south zone", "east zone")
#CANNOT BE DONE W FUNCTION... MUST BE DONE MANUALLY

personAndWeightDF <- data.frame()
for(i in 1:nrow(responsesDF)){
  #must be done manually, cannot use a loop. sadly
  #inner town
  personAndWeightDF[nrow(personAndWeightDF)+1, "email_utilisateur"] <- responsesDF$Wingz.Email.Address[i]
  personAndWeightDF[nrow(personAndWeightDF), "id_zone"] <- "WINGZAROUND_AUSTIN_DOWTOWN"
  if(is.na(getDriverID(responsesDF$Wingz.Email.Address[i])))
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- 0
  else
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- getDriverID(responsesDF$Wingz.Email.Address[i])
  personAndWeightDF[nrow(personAndWeightDF), "weight"] <- weightFunction(responsesDF$Your.Geo.Zone.Selection..1..Inner.town.[i])
  #outer town
  personAndWeightDF[nrow(personAndWeightDF)+1, "email_utilisateur"] <- responsesDF$Wingz.Email.Address[i]
  personAndWeightDF[nrow(personAndWeightDF), "id_zone"] <- "WINGZAROUND_AUSTIN_BORDER"
  if(is.na(getDriverID(responsesDF$Wingz.Email.Address[i])))
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- 0
  else
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- getDriverID(responsesDF$Wingz.Email.Address[i])
  personAndWeightDF[nrow(personAndWeightDF), "weight"] <- weightFunction(responsesDF$Your.Geo.Zone.Selection..2..Outer.town.[i])
  #northeast
  personAndWeightDF[nrow(personAndWeightDF)+1, "email_utilisateur"] <- responsesDF$Wingz.Email.Address[i]
  personAndWeightDF[nrow(personAndWeightDF), "id_zone"] <- "WINGZAROUND_AUSTIN_NORTHEAST"
  if(is.na(getDriverID(responsesDF$Wingz.Email.Address[i])))
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- 0
  else
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- getDriverID(responsesDF$Wingz.Email.Address[i])
  personAndWeightDF[nrow(personAndWeightDF), "weight"] <- weightFunction(responsesDF$Your.Geo.Zone.Selection..3..Northeast.[i])
  #northwest
  personAndWeightDF[nrow(personAndWeightDF)+1, "email_utilisateur"] <- responsesDF$Wingz.Email.Address[i]
  personAndWeightDF[nrow(personAndWeightDF), "id_zone"] <- "WINGZAROUND_AUSTIN_NORTHWEST"
  if(is.na(getDriverID(responsesDF$Wingz.Email.Address[i])))
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- 0
  else
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- getDriverID(responsesDF$Wingz.Email.Address[i])
  personAndWeightDF[nrow(personAndWeightDF), "weight"] <- weightFunction(responsesDF$Your.Geo.Zone.Selection..4..Northwest.[i])
  #west zone
  personAndWeightDF[nrow(personAndWeightDF)+1, "email_utilisateur"] <- responsesDF$Wingz.Email.Address[i]
  personAndWeightDF[nrow(personAndWeightDF), "id_zone"] <- "WINGZAROUND_AUSTIN_WEST"
  if(is.na(getDriverID(responsesDF$Wingz.Email.Address[i])))
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- 0
  else
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- getDriverID(responsesDF$Wingz.Email.Address[i])
  personAndWeightDF[nrow(personAndWeightDF), "weight"] <- weightFunction(responsesDF$Your.Geo.Zone.Selection..5..West.Zone.[i])
  #south zone
  personAndWeightDF[nrow(personAndWeightDF)+1, "email_utilisateur"] <- responsesDF$Wingz.Email.Address[i]
  personAndWeightDF[nrow(personAndWeightDF), "id_zone"] <- "WINGZAROUND_AUSTIN_SOUTH"
  if(is.na(getDriverID(responsesDF$Wingz.Email.Address[i])))
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- 0
  else
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- getDriverID(responsesDF$Wingz.Email.Address[i])
  personAndWeightDF[nrow(personAndWeightDF), "weight"] <- weightFunction(responsesDF$Your.Geo.Zone.Selection..6..South.Zone.[i])
  #east zone
  personAndWeightDF[nrow(personAndWeightDF)+1, "email_utilisateur"] <- responsesDF$Wingz.Email.Address[i]
  personAndWeightDF[nrow(personAndWeightDF), "id_zone"] <- "WINGZAROUND_AUSTIN_EAST"
  if(is.na(getDriverID(responsesDF$Wingz.Email.Address[i])))
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- 0
  else
    personAndWeightDF[nrow(personAndWeightDF), "id_utilisateur"] <- getDriverID(responsesDF$Wingz.Email.Address[i])
  personAndWeightDF[nrow(personAndWeightDF), "weight"] <- weightFunction(responsesDF$Your.Geo.Zone.Selection..7..East.Zone.[i])
}

#export to excel
write.xlsx(personAndWeightDF ,file = "Results.xlsx", sheetName = "RESULTS FOR BALVA", append=TRUE )
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
responsesDF <- read.csv("ResponsesNew.csv", header=TRUE)

#QUERY TO RETRIEVE DATABASE OF DRIVER/ZONE INFORMATION
realDatabaseDF <- dbGetQuery(myconn, "SELECT * FROM utilisateurs_has_zones LIMIT 100")
emailsDF <- dbGetQuery(myconn, "SELECT id_utilisateur, email FROM utilisateurs " )

#GRAB ALL EMAILS
allEmails <- data.frame(responsesDF$Wingz.Email.Address)
#REMOVE DUPLICATES
allEmails <- subset(allEmails, !duplicated(allEmails))
responsesDF <- subset(responsesDF, !duplicated(responsesDF$Wingz.Email.Address, fromLast=T))

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
    return(3)
  else if(entry == "Neutral")
    return(2)
  else if(entry == "Rejected")
    return(1)
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

personAndWeightDF[,"email_utilisateur"] <- sapply(personAndWeightDF[, "email_utilisateur"], as.character)

#if driver ID is missing, find it 
for(i in 1:length(personAndWeightDF$id_utilisateur)){
  if(personAndWeightDF$id_utilisateur[i] == 0)
  {
    indices <- which(responsesDF$Wingz.Email.Address == personAndWeightDF$email_utilisateur[i])
    nom <- responsesDF$Last.Name[indices[1]]
    prenom <- responsesDF$First.Name[indices[1]]
    query <- paste0("select * from utilisateurs where nom like '%",nom,"%' and prenom like '%", prenom,"%' and email_validation is not null")
    missingInfoDF <- dbGetQuery(myconn, query)
    #personAndWeightDF$email_utilisateur[i] <- missingInfoDF$email[1]
    personAndWeightDF$id_utilisateur[i] <- missingInfoDF$id_utilisateur[1]
  }
}



#cut off from previous old data
OldResponsesDF <- read.csv("OldResults.csv")

indices <- which(personAndWeightDF$email_utilisateur == as.character(OldResponsesDF$email_utilisateur[nrow(OldResponsesDF)]))
cutOffIndex <- indices[length(indices)]

personAndWeightDF1 <- personAndWeightDF[-c(1:980),]



#export to excel
write.xlsx(personAndWeightDF1 ,file = "NewResults.xlsx", sheetName = "RESULTS FOR BALVA", append=TRUE,row.names=FALSE )
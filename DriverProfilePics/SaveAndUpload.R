library(DBI)
library(chron)
library(parallel)
library(RMySQL)
library(RCurl)

rm(list=ls()) #clear Global Environment with each run of script
lapply(dbListConnections(dbDriver(drv="MySQL")), dbDisconnect)

#SET UP SQL CONNECTION
myconn <- dbConnect(RMySQL::MySQL(),dbname="wingz-prod",host="wingz-platform-read001.c8voyumknq5z.us-west-1.rds.amazonaws.com",
                    username="wingz-read-only", password="4P4v53S256hW7Z2X")

queryToGetPictures <- "select utilisateurs.id_utilisateur,utilisateurs.prenom,utilisateurs.nom,fs_images.user_filename, fs_images.id_fs_image
from utilisateurs
left join fs_images
on fs_images.id_fs_image = utilisateurs.id_picture
where utilisateurs.driver_activated_at is not NULL and utilisateurs.deleted_at is NULL and utilisateurs.id_driver_info is not NULL
"

pictureID_DF <- dbGetQuery(myconn, queryToGetPictures)


for(i in 1:length(pictureID_DF$id_fs_image)){
  if(is.na(pictureID_DF$id_fs_image[i]))
    next
  image_id <- pictureID_DF$id_fs_image[i]
  prenom <- pictureID_DF$prenom[i]
  nom <- pictureID_DF$nom[i]
  download.file(paste0("https://images-wingz.s3.amazonaws.com/p/",image_id,"/picture"), destfile = paste0("Pics/",prenom,nom,".jpg"),mode='wb')
}
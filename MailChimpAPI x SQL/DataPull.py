
# coding: utf-8

# In[33]:

import pandas as pd
import numpy as np
import csv
import pymysql as sql
import datetime


# In[34]:

#connect to company DB
conn = sql.connect(host='wingz-platform-read001.c8voyumknq5z.us-west-1.rds.amazonaws.com',
                            user='wingz-read-only', password='4P4v53S256hW7Z2X',
                            db='wingz-prod')


# In[35]:

#function to query results into a pandas dataframe
def query(query):
    return pd.read_sql(query, con=conn)

# In[36]:

#drivers dataframe
dfQuery = """
select distinct(id_utilisateur), id_driver_info 
from utilisateurs
"""
df = query(dfQuery)
#mark if driver or rider
def driverOrRider(x):
    if np.isnan(x):
        return 'rider'
    else:
        return 'driver'
df['Type'] = map(driverOrRider, df['id_driver_info'])
del df['id_driver_info']

#mark activated drivers
activatedDriversQuery = """
select id_utilisateur
from utilisateurs
where driver_activated_at is not NULL and deleted_at is NULL and id_driver_info is not NULL
"""
activatedDriversDF = query(activatedDriversQuery)
isActivated = lambda x : 1 if x in activatedDriversDF['id_utilisateur'].values else 0
df['is_activated_driver'] = df['id_utilisateur'].apply(isActivated)

#add number of rides booked, number of rides paid, and total cost
ridersInfoQuery = """select rider_id as 'id_utilisateur', COUNT(rider_id) as 'Rides Booked', 
sum(case when charged_to_rider = 0 then 0 else 1 end) as '# of rides paid',
SUM(charged_to_rider) as 'Total Cost'
from rides 
where provided is not null
group by rider_id
"""
ridersDF = query(ridersInfoQuery)
df = df.merge(ridersDF,how='left', on='id_utilisateur')

#calculate # of rides accepted
ridesAcceptedQuery = """
select driver_id as 'id_utilisateur', count(rides.driver_id) as "# of rides accepted"
from rides
where parent_id is not null and state_id in (6,7,8)
group by id_utilisateur
"""
ridesAcceptedDF = query(ridesAcceptedQuery)
df = df.merge(ridesAcceptedDF, how = 'left', on = 'id_utilisateur')

#calculate # of rides provided
ridesProvidedQuery = """
select driver_id as 'id_utilisateur', count(driver_id) as 'num_of_rides_provided'
from rides
where date_accepted is not null and provided = 1
group by driver_id
"""
ridesProvidedDF = query(ridesProvidedQuery)
df = df.merge(ridesProvidedDF, how = 'left', on = 'id_utilisateur')

#calculate # of days since last booking
lastBookingQuery = """
select rider_id as 'id_utilisateur', max(date_reservation) as "last booking"
from rides
group by rider_id
"""
#problem. datetime.datetime.now() calculates a time that is based on my computer's timezone. May be different
#than the recorded time in the database for an accepted ride. Therefore, the time difference is off
lastBookingDF = query(lastBookingQuery)
lastBookingDF['# of days since last booking'] = datetime.datetime.now() - lastBookingDF['last booking']
df = df.merge(lastBookingDF, how = 'left', on = 'id_utilisateur')

#calculate # of days since last ride accepted
lastRideAcceptedQuery = """
select driver_id as 'id_utilisateur', max(rides.date_accepted) as "last_accepted"
from rides
where rides.parent_id is NOT null AND rides.state_id in (6,7,8)
group by driver_id
"""
lastRideAcceptedDF= query(lastRideAcceptedQuery)
lastRideAcceptedDF['# of days since last ride accepted'] = datetime.datetime.now() - lastRideAcceptedDF['last_accepted'] 
#problem. datetime.datetime.now() calculates a time that is based on my computer's timezone. May be different
#than the recorded time in the database for an accepted ride. Therefore, the time difference is off
df = df.merge(lastRideAcceptedDF, how = 'left', on = 'id_utilisateur')

#calculate first reservation date
firstReservDateQuery = """
select rider_id as 'id_utilisateur', min(date_reservation) as "first_reservation"
from rides
group by rider_id
"""
firstReservDateDF = query(firstReservDateQuery)
df = df.merge(firstReservDateDF, how='left', on='id_utilisateur')

#calculate first ride provided ever from driver
firstRideQuery = """
select driver_id as 'id_utilisateur', min(rides.date_accepted) as "first_accepted"
from rides
where rides.parent_id is NOT null AND rides.state_id in (6,7,8)
group by driver_id
"""
firstRideDF = query(firstRideQuery)
df = df.merge(firstRideDF, how = 'left', on = 'id_utilisateur')

#signup platform (ios, android, api)
signUpPlatformQuery = """
select id_utilisateur, signup_platform
from utilisateurs
where signup_platform is not null
"""
signUpPlatformDF = query(signUpPlatformQuery)
df = df.merge(signUpPlatformDF, how = 'left', on = 'id_utilisateur')

#gender
genderQuery = """
select id_utilisateur, id_gender 
from utilisateurs
where id_gender is not null
"""
genderDF = query(genderQuery)
def maleOrFemale(x):
    if x == 1:
        return 'male'
    else:
        return 'female'
genderDF['id_gender'] = map(maleOrFemale, genderDF['id_gender'])
df = df.merge(genderDF, how = 'left', on = 'id_utilisateur')

#birthday 
birthdayQuery = """
select id_utilisateur, date_naissance as 'date_of_birth'
from utilisateurs
"""
birthdayDF = query(birthdayQuery)
df = df.merge(birthdayDF, how = 'left', on = 'id_utilisateur')

#location
locationQuery = """
select id_utilisateur, location
from utilisateurs
"""
locationDF = query(locationQuery)
df = df.merge(locationDF, how = 'left', on = 'id_utilisateur')

#referred user ID and count
referredByQuery = """
select id_utilisateur, id_utilisateur_parrain as 'referred_by'
from utilisateurs
"""
referredByDF= query(referredByQuery)
df = df.merge(referredByDF, how = 'left', on = 'id_utilisateur')

#referred count 
referredCount = """
select id_utilisateur_parrain as 'id_utilisateur', count(id_utilisateur) as 'reffered_amount'
from utilisateurs
where id_utilisateur_parrain is not null
group by id_utilisateur_parrain
"""
referredCountDF = query(referredCount)
df = df.merge(referredCountDF, how = 'left', on = 'id_utilisateur')

#disabled # of wheelchair access requests
disabledAccessQuery = """
select id_utilisateur, count(filter_wheelchair_accessible_vehicle) as 'num_of_wheelchair_access_requests'
from reservations
where filter_wheelchair_accessible_vehicle = 1
group by id_utilisateur
"""
disabledAccessDF = query(disabledAccessQuery)
df = df.merge(disabledAccessDF, how = 'left', on = 'id_utilisateur')

#riders: last date of rating submission
lastRiderReviewSubmission = """
select id_utilisateur, max(date_review_passenger) as 'last_rider_review_submission'
from reservations
where date_review_passenger is not null
group by id_utilisateur
"""
lastRiderReviewDF = query(lastRiderReviewSubmission)
df = df.merge(lastRiderReviewDF, how = 'left', on = 'id_utilisateur')

#drivers: average rating
avgDriverRatingQ = """
SELECT
                                                 id_utilisateur_conducteur          AS 'id_utilisateur',
                                                avg(reservations.rating_passenger) AS avg_driver_rating
                                                 FROM reservations
                                                INNER JOIN trajets ON reservations.id_trajet = trajets.id_trajet
                                               WHERE reservations.rating_passenger IS NOT NULL
                                                GROUP BY 1
"""
avgDriverRatingDF = query(avgDriverRatingQ)
df = df.merge(avgDriverRatingDF, how = 'left', on = 'id_utilisateur')

#drivers: airport selected
driverAirportQ = """
SELECT
drivers.id as 'id_utilisateur'
, drivers.airports
FROM (select
utilisateurs.id_utilisateur as id,
utilisateurs.prenom as first_name,
utilisateurs.nom as last_name,
utilisateurs.email,
utilisateurs.username,
utilisateurs.tel_mobile,
driver_account_types.code as account_type,
trajets.latitude_depart as latitude,
trajets.longitude_depart as longitude,
ifnull(utilisateurs_historiques.driver_activated_at, utilisateurs.driver_activated_at) as activated_at,
case when utilisateurs.power_driver_until is not null then 'Y' else 'N' end as power_driver_status,
drivers_info.created_at as apply_at,
group_concat(utilisateurs_has_airports.id_airport ORDER BY utilisateurs_has_airports.id_airport) as airports,
group_concat(distinct airports.market order by market) as markets,
utilisateurs.created_at,
utilisateurs.deleted_at
from utilisateurs
inner join drivers_info on utilisateurs.id_driver_info = drivers_info.id_driver_info
inner join driver_account_types on drivers_info.id_driver_account_type = driver_account_types.id_driver_account_type
left join trajets on utilisateurs.id_utilisateur = trajets.id_utilisateur_conducteur and trajets.id_type_trajet = 3 and trajets.id_etat_trajet = 1
left join utilisateurs_has_airports on utilisateurs.id_utilisateur = utilisateurs_has_airports.id_utilisateur
left join airports on utilisateurs_has_airports.id_airport = airports.id_airport
LEFT JOIN
                                              (SELECT
                                              id_utilisateur,
                                              min(driver_activated_at) AS driver_activated_at
                                              FROM utilisateurs_historiques
                                              WHERE id_driver_info IS NOT NULL
                                              GROUP BY 1) AS utilisateurs_historiques ON utilisateurs.id_utilisateur = utilisateurs_historiques.id_utilisateur
group by utilisateurs.id_utilisateur) as drivers
 LEFT JOIN (#only displays drivers who have referred riders through WingzForce program
select drivers.id
  , drivers.first_name
  , drivers.last_name
, drivers.tel_mobile as driver_phone_number
, drivers.email as email_address
, drivers.activated_at
, drivers.power_driver_status
, drivers.markets
, drivers.airports
  , concat('https://wingz.me/p/',drivers.username) as driver_page_url
, count(distinct wingzforce_drivers.rider_id) as referrals_num
,  sum(case when first_rides.booking_type='direct' then 1 else 0 end) as direct_bookings_referral
, sum(case when first_rides.booking_type='standard' then 1 else 0 end) as standard_bookings_referral
, count(first_rides.first_ride_id)*5 as referral_credit_balance
, max(first_rides.first_ride_date) as most_recent_referral_date
from (select
utilisateurs.id_utilisateur as id,
utilisateurs.prenom as first_name,
utilisateurs.nom as last_name,
utilisateurs.email,
utilisateurs.username,
utilisateurs.tel_mobile,
driver_account_types.code as account_type,
trajets.latitude_depart as latitude,
trajets.longitude_depart as longitude,
ifnull(utilisateurs_historiques.driver_activated_at, utilisateurs.driver_activated_at) as activated_at,
case when utilisateurs.power_driver_until is not null then 'Y' else 'N' end as power_driver_status,
drivers_info.created_at as apply_at,
group_concat(utilisateurs_has_airports.id_airport ORDER BY utilisateurs_has_airports.id_airport) as airports,
group_concat(distinct airports.market order by market) as markets,
utilisateurs.created_at,
utilisateurs.deleted_at
from utilisateurs
inner join drivers_info on utilisateurs.id_driver_info = drivers_info.id_driver_info
inner join driver_account_types on drivers_info.id_driver_account_type = driver_account_types.id_driver_account_type
left join trajets on utilisateurs.id_utilisateur = trajets.id_utilisateur_conducteur and trajets.id_type_trajet = 3 and trajets.id_etat_trajet = 1
left join utilisateurs_has_airports on utilisateurs.id_utilisateur = utilisateurs_has_airports.id_utilisateur
left join airports on utilisateurs_has_airports.id_airport = airports.id_airport
LEFT JOIN
                                              (SELECT
                                              id_utilisateur,
                                              min(driver_activated_at) AS driver_activated_at
                                              FROM utilisateurs_historiques
                                              WHERE id_driver_info IS NOT NULL
                                              GROUP BY 1) AS utilisateurs_historiques ON utilisateurs.id_utilisateur = utilisateurs_historiques.id_utilisateur
group by utilisateurs.id_utilisateur) as drivers
INNER JOIN (SELECT riders.referrer_id, riders.rider_id
             FROM (select
utilisateurs.id_utilisateur as rider_id,
utilisateurs.prenom as first_name,
utilisateurs.nom as last_name,
utilisateurs.email,
id_utilisateur_parrain as referrer_id,
facebook_id,
location_latitude as latitude,
location_longitude as longitude,
mobile_country as country,
signup_platform as platform,
min(ride_requests.id) as first_ride_request_id,
first_ride_completed_date,
first_ride_completed_id,
rides_completed_num,
utilisateurs.created_at,
utilisateurs.deleted_at,
tripit_users.id_tripit_user
from utilisateurs
left join (select *
from rides
where parent_id is null
and state_id in (5,6)) as ride_requests on utilisateurs.id_utilisateur = ride_requests.rider_id
left join (SELECT rider_id, count(*) as rides_completed_num, min(rides.departure_date_utc) as first_ride_completed_date, min(rides.id) as first_ride_completed_id
           FROM rides
           WHERE provided = 1
           GROUP BY 1) rides_provided on utilisateurs.id_utilisateur = rides_provided.rider_id
left join tripit_users on utilisateurs.id_utilisateur = tripit_users.id_utilisateur
where utilisateurs.id_driver_info is null
and utilisateurs.created_at >= '2013-08-01'
-- and utilisateurs.deleted_at is null
-- and id_business_provider is null
group by 1) as riders
             INNER JOIN (select *
from rides
where provided = 1) as rides_provided on riders.rider_id = rides_provided.rider_id
             WHERE riders.created_at >= '2016-03-01'
              ) as wingzforce_drivers on drivers.id = wingzforce_drivers.referrer_id
INNER JOIN  (SELECT rides.rider_id
             , min(rides.id) as first_ride_id
             , min(rides.departure_date_utc) as first_ride_date
             , rides.booking_type
    FROM rides
    WHERE provided=1
    GROUP BY 1) as first_rides ON wingzforce_drivers.rider_id = first_rides.rider_id
GROUP BY 1) as wingzforce_drivers ON drivers.id = wingzforce_drivers.id
LEFT JOIN (SELECT driver_id
           , sum(case when rides_provided.booking_type='direct' then 1 else 0 end) as direct_bookings
, sum(case when rides_provided.booking_type='standard' then 1 else 0 end) as standard_bookings
           , count(*) as total_rides_provided_ever
           FROM (select *
from rides
where provided = 1) as rides_provided
          GROUP BY 1) as rides_provided ON drivers.id = rides_provided.driver_id
WHERE drivers.activated_at IS NOT NULL
GROUP BY 1
ORDER BY total_rides_provided_ever DESC
"""
driverAirportSelectDF = query(driverAirportQ)
df = df.merge(driverAirportSelectDF, how = 'left', on = 'id_utilisateur')
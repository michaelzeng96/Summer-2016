SELECT sub.nom, sub.prenom, rides.driver_id, count(rides.driver_id) AS "# of rides accepted", 
sum(case when unavailable_requested_driver_fallback = 0 then 1 else 0 end) as "# of rides provided"
FROM rides, (select utilisateurs.id_utilisateur,utilisateurs.prenom,utilisateurs.nom
from utilisateurs
where utilisateurs.driver_activated_at is not NULL and utilisateurs.deleted_at is NULL and utilisateurs.id_driver_info is not NULL) sub
WHERE parent_id is NOT null AND state_id in (6,7,8) AND date_reservation > '2016-01-01'
and rides.driver_id = sub.id_utilisateur
group by driver_id

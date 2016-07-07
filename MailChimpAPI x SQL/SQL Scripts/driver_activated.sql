select utilisateurs.id_utilisateur,utilisateurs.prenom,utilisateurs.nom
from utilisateurs
where utilisateurs.driver_activated_at is not NULL and utilisateurs.deleted_at is NULL and utilisateurs.id_driver_info is not NULL

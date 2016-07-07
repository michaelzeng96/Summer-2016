select utilisateurs.id_utilisateur,utilisateurs.prenom,utilisateurs.nom
from utilisateurs
where utilisateurs.deleted_at is NULL and utilisateurs.id_driver_info is not NULL



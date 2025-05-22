podman exec -it postgres psql -U admin -d spond_prod -a -f /tmp/db-setup/setup.sql;

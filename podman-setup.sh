podman pod create --name py-pg-case-pod -p 5432:5432;

podman run -d \
  --name postgres \
  --pod py-pg-case-pod \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=g638ab94C3WysbXBG \
  -e POSTGRES_DB=spond_prod \
  docker.io/library/postgres:15;

podman cp db-setup postgres:/tmp/db-setup;

python -m venv venv;
source venv/bin/activate;
pip install -r requirements.txt;

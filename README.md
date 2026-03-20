##### 5214g7project

##### ===============================
##### Setup
##### ===============================

python -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

git checkout -b dev_你的名字

docker compose up -d redis zookeeper kafka1 kafka2 kafka3 clickhouse mysql mongodb

docker compose logs -f kafka1 clickhouse

docker compose run --rm init_infra

docker compose up -d app celery_worker celery_beat metabase


##### ===============================
##### Restart
##### ===============================

docker compose restart celery_worker celery_beat

docker compose logs -f celery_worker


##### ===============================
##### Daily Dev Workflow
##### ===============================
##### After changing Python code / tasks
docker compose restart celery_worker celery_beat


##### ===============================
##### After changing dependencies (requirements.txt)
##### ===============================
docker compose build

docker compose up -d


##### ===============================
##### After changing Dockerfile
##### ===============================
docker compose build --no-cache

docker compose up -d


##### ===============================
##### After changing docker-compose.yml
##### ===============================
docker compose down

docker compose up -d


##### ===============================
##### Debugging
##### ===================
##### View logs
docker compose logs -f celery_worker

docker compose logs -f celery_beat

##### Enter container
docker exec -it polymarket_celery_worker bash


##### ===============================
##### Full reset (if things break)
##### ===============================
docker compose down

docker compose up -d

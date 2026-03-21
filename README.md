##### 5214g7project

##### Setup

python -m venv .venv
# py -3.12 -m venv .venv

source .venv/bin/activate
# .venv\Scripts\Activate.ps1

pip install -r requirements.txt

git checkout -b dev_你的名字

docker compose up -d redis zookeeper kafka1 kafka2 kafka3 clickhouse mysql mongodb

docker compose run --rm init_infra

docker compose up -d app celery_worker celery_beat metabase

##### Logs

docker compose logs -f kafka1 clickhouse

docker compose logs -f celery_beat

##### Restart

docker compose restart celery_worker celery_beat

docker compose logs -f celery_worker


##### Daily Dev Workflow
##### After changing Python code / tasks
docker compose restart celery_worker celery_beat


##### After changing dependencies (requirements.txt)
docker compose build

docker compose up -d


##### After changing Dockerfile
docker compose build --no-cache

docker compose up -d


##### After changing docker-compose.yml
docker compose down

docker compose up -d


##### Debugging
##### View logs
docker compose logs -f celery_worker

docker compose logs -f celery_beat

##### Enter container
docker exec -it polymarket_celery_worker bash


##### Full reset (if things break)
docker compose down

docker compose up -d


##### List Kafka topics
docker exec -it polymarket_kafka1 bash

kafka-topics --bootstrap-server kafka1:19092 --list


##### ClickHouse
docker exec -it polymarket_clickhouse bash

clickhouse-client

SHOW DATABASES;

USE default;

SHOW TABLES;


##### metabase
docker compose down

docker compose up -d

docker exec -it polymarket_mysql mysql -u root -p

password: password

CREATE DATABASE IF NOT EXISTS metabase;

GRANT ALL PRIVILEGES ON metabase.* TO 'polymarket_user'@'%';

FLUSH PRIVILEGES;

docker compose restart metabase

http://localhost:3000

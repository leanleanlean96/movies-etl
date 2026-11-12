# Локальный стенд

[Основной README](../README.md) · [Airflow](../airflow/README.md) ·
[ClickHouse](clickhouse/README.md)

## Подготовка

Команды выполняются из корня репозитория. Нужны Docker Engine с Compose v2,
доступ к `/var/run/docker.sock` и интернет для сборки образов и запросов API.
Cloudberry использует Ubuntu-пакет amd64; на другой архитектуре нужна
соответствующая среда запуска. Spark worker настроен на 2 ядра и 4 ГБ памяти;
помимо него одновременно работают драйверы, базы и Airflow, поэтому стенду
нужна дополнительная память.

```bash
cp .env.example .env
id -u
stat -c '%g' /var/run/docker.sock
pwd
```

В `.env` перенесите UID в `AIRFLOW_UID`, группу socket в `DOCKER_GID`, абсолютный
путь из `pwd` в `ETL_PROJECT_ROOT`. Путь должен существовать на Docker host:
DockerOperator использует его для bind mounts временных Spark/dbt-контейнеров.
Для Linux создайте доступный Airflow каталог логов:

```bash
mkdir -p airflow/logs
```

Владелец каталога должен соответствовать `AIRFLOW_UID`. Значение 1000
из примера подходит только пользователю с таким UID.

## Переменные окружения

| Переменные | Назначение |
| --- | --- |
| `AIRFLOW_UID`, `DOCKER_GID`, `ETL_PROJECT_ROOT` | Права Airflow и доступ к Docker/bind mounts |
| `AIRFLOW_DB_PASSWORD` | Пароль служебной БД PostgreSQL |
| `AIRFLOW_ADMIN_PASSWORD` | Пароль создаваемого при инициализации пользователя `admin` |
| `AIRFLOW_FERNET_KEY` | Fernet-ключ шифрования Airflow; требуется корректный ключ, не `change-me` |
| `AIRFLOW_JWT_SECRET` | Случайный секрет API Airflow |
| `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` | MinIO Console, S3 connection Airflow и Spark |
| `CLOUDBERRY_PASSWORD` | Пароль `gpadmin`; entrypoint допускает только латинские буквы и цифры |
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | Доступ к ClickHouse; пример использует `etl` |
| `KINOPOISK_API_KEY` | Ключ PoiskKino; обязателен для извлечения, replay его не использует |
| `AIRFLOW_ALERT_EMAIL` | Получатели ошибки daily DAG через запятую; пустое значение отключает отправку |
| `AIRFLOW_PUBLIC_URL` | Внешний адрес Airflow для ссылок в письмах |
| `AIRFLOW_CONN_SMTP_ALERTS` | SMTP connection JSON; вместо переменной можно создать `smtp_alerts` в UI |

Ключи можно сгенерировать через временный контейнер:

```bash
docker run --rm apache/airflow:3.3.0-python3.11 \
  python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
docker run --rm python:3.11-slim \
  python -c 'import secrets; print(secrets.token_hex(32))'
```

Первый вывод — `AIRFLOW_FERNET_KEY`, второй — `AIRFLOW_JWT_SECRET`.
SMTP JSON и примеры SSL/STARTTLS приведены в [`.env.example`](../.env.example).
При смене `CLICKHOUSE_USER` согласуйте имя пользователя в
[`refresh.xml`](clickhouse/users.d/refresh.xml), где права заданы для `etl`.

## Первый запуск

Сначала проверьте конфигурацию без вывода подставленных секретов:

```bash
docker compose config --quiet
docker compose build
docker compose --profile tools build dbt spark-submit
docker compose up -d
docker compose ps -a
```

`airflow-init` мигрирует служебную БД и создаёт `admin`; `minio-init` создаёт
бакеты `etl-raw` и `etl-cleaned`. Для этих двух сервисов ожидается завершение
с кодом 0. Первый старт Cloudberry занимает время: создаётся локальный кластер
без зеркал с двумя primary-сегментами и БД `etl`. Дождитесь `healthy` у
Cloudberry, ClickHouse и Airflow API, а также успешного завершения init-сервисов.

DDL таблиц данных не применяется entrypoint автоматически. Выполните по порядку:

```bash
docker compose exec -T --user gpadmin cloudberry bash -lc \
  'source /usr/local/cloudberry-db/cloudberry-env.sh && psql -p 7000 -d etl -v ON_ERROR_STOP=1' \
  < infra/cloudberry/sql/staging.sql
docker compose exec -T --user gpadmin cloudberry bash -lc \
  'source /usr/local/cloudberry-db/cloudberry-env.sh && psql -p 7000 -d etl -v ON_ERROR_STOP=1' \
  < infra/cloudberry/sql/dbt_inputs.sql
```

Создайте core/mart на текущем staging. На новом стенде он пустой; эта сборка
также создаёт источники для последующей настройки ClickHouse:

```bash
docker compose run --rm --entrypoint python dbt scripts/build.py
```

Затем создайте внешние таблицы и локальные витрины ClickHouse:

```bash
docker compose exec -T clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < infra/clickhouse/sql/01_sources.sql
docker compose exec -T clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < infra/clickhouse/sql/02_views.sql
docker compose exec -T airflow-scheduler airflow pools set \
  kinopoisk_processing 1 'Serialize daily and replay processing'
```

DDL ClickHouse создаёт пустые витрины с остановленным refresh. Публикация
происходит после успешного dbt в DAG либо вручную по
[инструкции ClickHouse](clickhouse/README.md).

Проверьте DAG и включите ежедневную загрузку:

```bash
docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose exec -T airflow-scheduler airflow dags list
docker compose exec -T airflow-scheduler airflow dags unpause kinopoisk_movies_daily
```

Включение daily DAG запускает catchup с 2026-10-20 UTC и расходует API-запросы.
Подробности расписания и охвата — в [Airflow README](../airflow/README.md).
Replay включается отдельно и требует URI существующего raw-манифеста.

## Обновление и остановка

Изменения Python-кода в монтируемых каталогах доступны контейнерам без
пересборки. После изменения Dockerfile пересоберите соответствующий образ и
пересоздайте сервисы. После изменения `.env` выполните `docker compose up -d`,
чтобы сервисы получили новые значения. Образы dbt/Spark нужны заранее:
DockerOperator запускает их по тегу и сам не собирает.

```bash
docker compose stop
docker compose up -d
```

Для удаления контейнеров и сети с сохранением данных:

```bash
docker compose down
```

Данные находятся в named volumes `airflow-db-data`, `minio-data`,
`cloudberry-data`, `clickhouse-data`; логи Airflow — в `airflow/logs`.
`docker compose down -v` удаляет volumes вместе с данными.

## Диагностика

```bash
docker compose ps -a
docker compose logs --tail=100 airflow-init minio-init
docker compose logs --tail=100 airflow-scheduler airflow-dag-processor
docker compose logs --tail=100 cloudberry clickhouse spark-worker
docker compose exec -T airflow-scheduler airflow pools list
```

Логи временных драйверов и dbt ищите в соответствующих задачах Airflow:
контейнеры DockerOperator удаляются после завершения. Если задача не стартует,
проверьте наличие pool, образа, сеть `etl-pet-net`, группу Docker socket и
`ETL_PROJECT_ROOT`. Если staging отсутствует — примените оба Cloudberry DDL
выше. Если публикация ClickHouse падает — проверьте DDL, доступ к Cloudberry
и [состояние refresh](clickhouse/README.md).

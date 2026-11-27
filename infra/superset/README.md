# Apache Superset

Superset строит графики и дашборды по опубликованным витринам ClickHouse.
В Compose профиль `bi` добавляет веб-приложение и PostgreSQL для его метаданных.

## Запуск

Заполните `SUPERSET_SECRET_KEY`, `SUPERSET_DB_PASSWORD` и
`SUPERSET_ADMIN_PASSWORD` в `.env`. Для ключа и каждого пароля можно отдельно
выполнить `openssl rand -hex 32`. Сохраняйте один и тот же `SUPERSET_SECRET_KEY`
между запусками. Superset использует его для шифрования учётных данных источников.

Из корня проекта соберите образ и дождитесь готовности сервиса.

```bash
docker compose --profile bi up -d --build --wait superset
docker compose --profile bi ps -a superset superset-init superset-db
```

При повторном запуске с уже собранным образом можно использовать `--no-build`
вместо `--build`.

`superset-init` применяет миграции, обновляет роли и при первом запуске создаёт
пользователя `admin`. После завершения `superset-init` с кодом 0 и перехода
`superset` в `healthy` откройте **http://localhost:8088** и войдите как `admin`
с паролем `SUPERSET_ADMIN_PASSWORD`. После создания пользователя меняйте его
пароль через интерфейс Superset. Дашборды, датасеты и подключения сохраняются
в томе `superset-db-data`.

## Подключение к ClickHouse

На работающем ClickHouse создайте пользователя Superset с доступом к таблицам
витрин. Откройте клиент под администратором ClickHouse:

```bash
docker compose exec clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD"'
```

Замените `your_generated_password` отдельным паролем для `superset_reader`.

```sql
CREATE USER IF NOT EXISTS superset_reader IDENTIFIED WITH sha256_password BY 'your_generated_password';
GRANT SELECT ON mart.mart_* TO superset_reader;
```

В Superset откройте **Settings → Database Connections → + Database**. Выберите
**ClickHouse** и переключитесь на подключение по **SQLAlchemy URI**. Проверьте
подключение через **Test Connection**, затем сохраните его.

```text
clickhousedb://superset_reader:<пароль>@clickhouse:8123/mart
```

Для специальных символов в пароле используйте URL-кодирование. Здесь
`clickhouse:8123` — адрес сервиса внутри сети Compose.

После [создания и публикации витрин](../clickhouse/README.md) проверьте запрос в
**SQL Lab** или добавьте `mart.mart_movie_scores_current` через **Datasets**.

```sql
SELECT count() FROM mart.mart_movie_scores_current;
```

## Остановка

Чтобы остановить BI и сохранить подключения и дашборды, выполните:

```bash
docker compose --profile bi stop superset superset-db
```

## Диагностика

```bash
docker compose --profile bi logs --tail=100 superset-init superset superset-db
```

Описание драйвера и формата подключения — в
[документации Superset](https://superset.apache.org/docs/databases/supported/clickhouse/).

# Разработка и тестирование

[Основной README](../README.md) · [Подготовка стенда](../infra/README.md)

Команды выполняются из корня репозитория. Локальная версия Python — 3.11
(`.python-version`, ограничение `<3.12` в `pyproject.toml`). Docker-окружения
устанавливают собственные зависимости из Dockerfile; `uv sync` их не меняет.

## Локальные проверки

```bash
uv sync --frozen
uv run ruff check .
uv run pytest -q
```

pytest добавляет `src` и `spark/jobs` в Python path. Большинство Spark-тестов
использует общую сессию `local[2]` и не требует Docker, MinIO или API,
но требует установленной Java, совместимой с PySpark 4.2.0, и корректного
`JAVA_HOME`. Контейнерный Spark использует Python 3.10; при изменении jobs
сохраняйте эту совместимость (она учтена в настройках Ruff).

Быстрая проверка клиента, записи raw, интервалов и replay без запуска Spark:

```bash
uv run pytest -q tests/test_kinopoisk.py tests/test_partition_plan.py \
  tests/test_extract.py tests/test_interval.py tests/test_replay.py
```

Обычный `pytest` пропускает интеграционные наборы, пока не включены флаги ниже.
Проверки API используют подставные ответы, реальный ключ для них не нужен.

## Интеграционные проверки

Сначала поднимите Cloudberry и, для публикации, ClickHouse по
[инструкции стенда](../infra/README.md). Тесты создают одноразовые базы
с уникальными именами и удаляют их после завершения; нужны права создания
и удаления баз. Основная БД `etl` не используется как тестовая.

Compose читает `.env` автоматически, локальный pytest — нет. Экспортируйте
`CLOUDBERRY_PASSWORD`, а для ClickHouse также `CLICKHOUSE_USER` и
`CLICKHOUSE_PASSWORD` со значениями своего стенда. Для Cloudberry задайте:

```bash
export CLOUDBERRY_HOST=127.0.0.1
export CLOUDBERRY_PORT=7000
export CLOUDBERRY_USER=gpadmin
```

Не используйте Docker-имя `cloudberry` для подключения с хоста.

### Загрузка staging

Нужен PostgreSQL JDBC JAR в classpath локального Spark. Например, после сборки
образа его можно скопировать и передать через `PYSPARK_SUBMIT_ARGS`:

```bash
mkdir -p /tmp/etl-pet-test-jars
docker compose run --rm --entrypoint cat spark-submit \
  /opt/spark/jars/postgresql-42.7.14.jar \
  > /tmp/etl-pet-test-jars/postgresql-42.7.14.jar
PYSPARK_SUBMIT_ARGS='--jars /tmp/etl-pet-test-jars/postgresql-42.7.14.jar pyspark-shell' \
  RUN_CLOUDBERRY_TESTS=1 uv run pytest -q tests/integration/test_cleaned_to_stg_cloudberry.py
```

Проверяются повторная замена батча, сохранение других батчей и откат при
ошибках загрузки. Подключение Spark к Cloudberry идёт через JDBC.

### Модели dbt

```bash
RUN_DBT_TESTS=1 uv run pytest -q tests/integration/test_dbt_models_cloudberry.py
```

Тесты сами задают профиль из `infra/dbt/profiles`, создают DDL входов и
используют временные каталоги артефактов dbt. Проверяют повтор/full-refresh,
поздние данные, конфликты, rollback после записи, удаления, наборы связей,
NULL/SCD1, снимки топа, недельные дельты и стандартную incremental-стратегию.

### Публикация ClickHouse

```bash
export CLICKHOUSE_HOST=127.0.0.1
export CLICKHOUSE_PORT=8123
RUN_PUBLISH_TESTS=1 uv run pytest -q tests/integration/test_publish_clickhouse.py
```

Нужны обе базы: ClickHouse читает временные таблицы Cloudberry через
`cloudberry:7000` внутри Docker-сети. Проверяются точность типов, повторы,
изменения, удаления, пустые снимки и восстановление после ошибки refresh.
Используются отдельные временные БД и named collection ClickHouse.

## SQL-проверки опубликованных моделей

```bash
docker compose run --rm dbt test
```

Это проверка текущего DWH, а не изолированного тестового набора.
Штатный `scripts/build.py` сам выполняет SQL data tests до общего commit;
ошибка отменяет публикацию core/mart. Подробнее — в [dbt README](../dbt/README.md).

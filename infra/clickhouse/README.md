# ClickHouse: публикация витрин

[Основной README](../../README.md) · [Запуск стенда](../README.md) ·
[Модели dbt](../../dbt/README.md)

ClickHouse 26.8.31.2 хранит восемь локальных `MergeTree`-таблиц в БД `mart`.
Их имена совпадают с восемью моделями `dbt/models/mart`. Данные читаются
непосредственно из Cloudberry через PostgreSQL engine; Python в Airflow
передаёт только управляющие SQL-запросы по HTTP.

## Настройка

- `config.d/cloudberry.xml` задаёт named collection `cloudberry_marts`:
  `cloudberry:7000`, БД `etl`, схема `mart`, пользователь `gpadmin`, пароль
  из `CLOUDBERRY_PASSWORD`.
- `sql/01_sources.sql` создаёт восемь внешних таблиц `ext_cloudberry.*`
  с явным контрактом типов.
- `sql/02_views.sql` создаёт локальные таблицы `mart.*` и refreshable MV
  `mart.refresh_<имя витрины>`, с `EMPTY` и остановленным refresh.
- `users.d/refresh.xml` останавливает автоматический refresh после рестарта
  сервера и задаёт `named_collection_control` пользователю `etl`.
  Если изменили `CLICKHOUSE_USER`, согласуйте этот XML.

Сначала создайте таблицы Cloudberry командой dbt, затем примените
`01_sources.sql` и `02_views.sql`: точные команды есть в
[инструкции первого запуска](../README.md). SQL-файлы монтируются в контейнер
в `/workspace/clickhouse`; при старте сервера автоматически не выполняются.
`CREATE IF NOT EXISTS` допускает повторное создание, но не мигрирует
существующую схему при изменении колонок. Контракты SQL нужно согласовывать
с dbt-моделями.

## Публикация

После успешной `build_dbt` задача `publish_clickhouse` последовательно
выполняет `sql/refresh.sql`. Для каждой витрины: STOP → REFRESH → START →
WAIT → STOP. Задача дожидается окончания обновления; в `finally` выполняет
`sql/stop.sql` для всех восьми view, в том числе при ошибке.

В SQL задан `max_execution_time=1200`, внутренние повторы refresh отключены.
В Airflow есть один повтор задачи через 5 минут и таймаут 30 минут на весь этап.
Декларация `REFRESH AFTER 1 DAY` не означает независимую ежедневную публикацию:
view штатно остановлены, ими управляет DAG.

Для ручной публикации после успешной сборки dbt, вне параллельной обработки:

```bash
docker compose exec -T clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < infra/clickhouse/sql/refresh.sql
```

При ошибке ручной команды выполните остановку view, которую DAG делает сам:

```bash
docker compose exec -T clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < infra/clickhouse/sql/stop.sql
```

## Атомарность и повторы

Успешный refresh полностью и атомарно заменяет содержимое **одной** витрины.
Удаления и пустой результат также публикуются. Ошибка refresh сохраняет прежнее
содержимое этой таблицы. Общей транзакции для восьми витрин нет: при ошибке
в середине публикации часть таблиц уже может содержать новые данные.
Повтор всей задачи заново обновляет все витрины и восстанавливает их состояние
по текущему Cloudberry.

Публикация не блокирует dbt или staging. Airflow pool сериализует отдельные
задачи обработки, но задачи разных DAG могут чередоваться. Ручная сборка dbt
во время refresh может привести к чтению разных версий DWH разными витринами.
Для согласованного ручного обновления сначала завершите dbt, затем refresh
при отсутствии других публикаций.

## Проверка состояния

```bash
docker compose exec -T clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT * FROM system.view_refreshes FORMAT Vertical"'
docker compose exec -T clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT count() FROM mart.mart_movie_scores_current"'
```

При ошибке проверьте лог задачи Airflow, `system.view_refreshes`, доступность
Cloudberry, наличие моделей `mart` и соответствие типов в `01_sources.sql`.
Интеграционные проверки публикации описаны в [tests README](../../tests/README.md).

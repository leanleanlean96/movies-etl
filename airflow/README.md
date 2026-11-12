# Оркестрация Airflow

[Основной README](../README.md) · [Запуск стенда](../infra/README.md) ·
[Replay](REPLAY.md)

Используются Airflow 3.3.0, LocalExecutor и FAB auth manager. Compose запускает
API server, scheduler, dag processor и отдельный PostgreSQL для метаданных.
Новые DAG создаются на паузе. DAG и плагины монтируются из этого каталога,
библиотека `movies_pipeline` — из `src`.

## Ежедневная загрузка

`kinopoisk_movies_daily` работает по `@daily` в UTC, начальная дата —
`2026-10-20`, `catchup=True`, `max_active_runs=1`. Интервал обработки берётся
из `data_interval_start` / `data_interval_end`; границы должны приходиться
на полночь UTC. В API они передаются как `DD.MM.YYYY-DD.MM.YYYY`.
Spark дополнительно отбирает строки по полуоткрытому интервалу `[start, end)`.

```text
land_movies → print_manifest_key → clean_movies → load_movies_stg
            → build_dbt → publish_clickhouse
```

| Задача | Результат |
| --- | --- |
| `land_movies` | Страницы JSONL.gz и завершающий raw-манифест в MinIO; XCom с URI, batch_id и счётчиками |
| `print_manifest_key` | URI и счётчики raw в логе |
| `clean_movies` | Spark driver в Docker, четыре cleaned Parquet-таблицы и манифест |
| `load_movies_stg` | Spark driver в Docker, атомарная замена трёх staging-таблиц для batch_id |
| `build_dbt` | `python scripts/build.py` во временном dbt-контейнере, общий commit core/mart |
| `publish_clickhouse` | Управляющие HTTP-запросы в ClickHouse; данные ClickHouse читает из Cloudberry сам |

Общая цепочка обработки определена в `plugins/kinopoisk_tasks.py` и используется
обоими DAG. Spark запускается в client mode на `spark://spark-master:7077`;
драйверы подключены к `etl-pet-net`. Bind mounts строятся по `ETL_PROJECT_ROOT`
на Docker host. Пароли в DockerOperator передаются через `private_environment`.

## Охват и бюджеты API

Клиент использует `https://api.poiskkino.dev/v1.5`, ключ `KINOPOISK_API_KEY`
и заголовок `X-API-KEY`. Daily DAG запрашивает `/movie`, `type=movie` и поля
из `MOVIE_FIELDS` в `src/movies_pipeline/sources/kinopoisk.py`.

Начальные партиции `votes.kp`: `1000–1999`, `2000–4999`, `5000–19999`,
`20000–100000000`. Фильмы вне этих границ не входят в текущую загрузку.
Перед загрузкой планировщик считает записи и при необходимости делит диапазоны:

- размер страницы — 250;
- максимум на один запрос-партицию — 10 страниц;
- целевое заполнение партиции — 90% от 2500 записей;
- бюджет планирования — 30 count-запросов;
- бюджет загрузки daily DAG — 100 страниц суммарно, включая пустые ответы.

Это ограничения текущего кода, а не гарантия актуальных условий тарифа API.
Превышение бюджета или невозможность дальнейшего деления завершает задачу
ошибкой; complete-манифест не публикуется. Клиент повторяет GET при 429,
500, 502, 503 и 504, учитывая `Retry-After`.

Catchup и повтор `land_movies` обращаются к текущему API. Новый запуск извлечения
создаёт новый `batch_id` по времени UTC с точностью до секунды. Для переработки
уже сохранённого наблюдения используйте [replay](REPLAY.md).

## Последовательность и повторы

Перед использованием обоих DAG создайте pool `kinopoisk_processing` с одним
слотом, как описано в [инструкции запуска](../infra/README.md).
Он назначен задачам `clean_movies`, `load_movies_stg`, `build_dbt`,
`publish_clickhouse`. В каждый момент выполняется одна такая задача, но между
задачами daily и replay запуски могут чередоваться. Pool не блокирует ручные
запуски вне Airflow; для согласованной публикации не запускайте их одновременно
с обработкой DAG.

У `land_movies` и четырёх задач обработки один повтор через 5 минут.
Таймаут staging — 20 минут, dbt и публикации — по 30 минут.
У `clean_movies` явный execution timeout не задан. При ошибке позднего этапа
предыдущие успешные этапы остаются опубликованными; можно повторить упавшую
задачу. Границы атомарности описаны в [Spark](../spark/README.md),
[dbt](../dbt/README.md) и [ClickHouse](../infra/clickhouse/README.md).

dbt читает весь staging и строит снимки до текущей UTC-даты запуска.
Дата старого интервала DAG не уменьшает уже рассчитанный горизонт.

## Уведомления

Daily DAG отправляет одно SMTP-уведомление при окончательной ошибке DAG,
после попыток задач. Задайте `AIRFLOW_ALERT_EMAIL` через запятую и connection
`smtp_alerts` в Airflow UI либо `AIRFLOW_CONN_SMTP_ALERTS` в `.env`.
`AIRFLOW_PUBLIC_URL` задаёт адрес ссылок в письмах. Пустой список получателей
отключает callback; у replay DAG такого callback нет.

## Ручная проверка

```bash
docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose exec -T airflow-scheduler airflow tasks list kinopoisk_movies_daily
# Выполняет настоящую сборку dbt на текущем staging, без извлечения из API:
docker compose exec -T airflow-scheduler \
  airflow tasks test kinopoisk_movies_daily build_dbt 2026-11-11
```

`tasks test` здесь запускает реальную задачу с записью в DWH.
Для изолированных проверок используйте [интеграционные тесты](../tests/README.md).

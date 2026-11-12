# Replay сохранённого raw-батча

[Основной README](../README.md) · [Airflow](README.md) ·
[Подготовка стенда](../infra/README.md)

Ручной DAG `kinopoisk_movies_replay` повторяет обработку сохранённого батча:
`validate_manifest → clean_movies → load_movies_stg → build_dbt → publish_clickhouse`.
Он не вызывает API. Укажите точный URI существующего `_manifest.json` из `etl-raw`.
`batch_id`, исходный интервал и время наблюдения сохраняются; staging заменяет
только этот батч. Spark проверяет полный контракт манифеста и checksum страниц
перед записью cleaned. Не используйте replay для восстановления отсутствующих raw-файлов.

Нужны инициализированные staging и ClickHouse, собранные Spark/dbt-образы,
доступ к MinIO и raw-манифест версии 2 со всеми страницами. Ключ API не требуется.
Контракты и расположение данных — в [Spark README](../spark/README.md).

Перед первым использованием на новом стенде создайте общий pool с одним слотом
(он нужен и ежедневному DAG):

```bash
docker compose exec -T airflow-scheduler airflow pools set kinopoisk_processing 1 'Serialize daily and replay processing'
```

Запуск из UI: включите `kinopoisk_movies_replay`, выберите Trigger DAG и заполните
`manifest_uri`. Через CLI, подставив URI своего батча:

```bash
docker compose exec -T airflow-scheduler airflow dags unpause kinopoisk_movies_replay
docker compose exec -T airflow-scheduler airflow dags trigger kinopoisk_movies_replay \
  --conf '{"manifest_uri":"s3://etl-raw/kinopoisk/movies/dt=2026-11-11/run=20261112T042321Z/_manifest.json"}'
```

Параметр обязателен при выполнении: пустой URI или незавершённый манифест
останавливают DAG до обработки. Для нескольких батчей создайте отдельные ручные
запуски; `max_active_runs=1` выполняет их последовательно. Общий pool исключает
одновременное выполнение задач обработки daily и replay; порядок задач разных
DAG может чередоваться между завершениями задач.

dbt пересчитывает core/mart по всему staging до текущей UTC-даты, ClickHouse
обновляет все витрины. Дата raw-интервала не передаётся в `snapshot_end_date`:
иначе существующий reconcile мог бы удалить более поздние снимки.
Replay восстанавливает обработку сохранённых наблюдений, а не состояние API
на произвольную прошлую дату.

Если упал только `build_dbt` или `publish_clickhouse`, можно повторить эту задачу
в существующем запуске. Полный replay заново перезаписывает cleaned и staging
батча. При ошибке загрузки staging сохраняет прежнюю опубликованную версию;
при ошибке dbt откатывается вся сборка core/mart. В ClickHouse часть витрин
к этому моменту уже может обновиться: повтор публикации обновляет все восемь.
Подробнее — [границы атомарности ClickHouse](../infra/clickhouse/README.md).

У replay DAG нет SMTP callback daily DAG. Ошибки и результаты смотрите
в Airflow UI и логах задач.

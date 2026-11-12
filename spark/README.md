# Spark: raw → cleaned → staging

[Основной README](../README.md) · [Запуск стенда](../infra/README.md) ·
[Тестирование](../tests/README.md)

Образ Spark 4.2.0 содержит S3A-коннектор, AWS SDK и PostgreSQL JDBC driver.
MinIO настроен в `infra/spark/spark-defaults.conf`; учётные данные приходят
через `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`. Сессии работают в UTC.

## Расположение и контракты данных

```text
s3://etl-raw/kinopoisk/movies/dt=YYYY-MM-DD/run=<batch_id>/
  page-00001.jsonl.gz
  ...
  _manifest.json

s3://etl-cleaned/kinopoisk/
  movies/batch_id=<batch_id>/
  movie_genres/batch_id=<batch_id>/
  movie_countries/batch_id=<batch_id>/
  quarantine/batch_id=<batch_id>/
  _manifests/batch_id=<batch_id>/_manifest.json
```

Raw-манифест версии **2** записывается после страниц и содержит
`status=complete`, идентификатор батча, bucket/prefix, интервал, параметры API,
список страниц, их размеры, количество записей и SHA-256 хранимых gzip-объектов.
Время `finished_at` raw используется как время наблюдения `ingested_at`.
При незавершённом извлечении страницы могут остаться без complete-манифеста.

Cleaned-манифест версии **1** содержит исходный URI и интервал, `batch_id`,
статистику обработки, URI, число строк и Spark-схему каждой из четырёх таблиц.
`s3://` и `s3a://` поддерживаются на входе Spark; файловый доступ идёт через S3A.

## Очистка

`jobs/kinopoisk_raw_to_cleaned.py` и пакет `jobs/kinopoisk_cleaned/`:

1. Проверяют контракт raw-манифеста, наличие страниц, размер и checksum.
2. Читают только перечисленные страницы с явной схемой и сверяют число строк
   каждой страницы с манифестом.
3. Выделяют карантин: `malformed_json`, `missing_id` (также неположительный ID),
   `bad_updated_at`, `outside_interval`, `unexpected_type`.
4. Среди валидных записей оставляют одну на `id`: максимальный
   `source_updated_at`, затем максимальный `source_page`, затем детерминированное
   сравнение содержимого строки.
5. Формируют плоскую `movies` и связи `movie_genres` / `movie_countries`,
   нормализуют строки и типы, удаляют дубли связей.
6. Перезаписывают Parquet-каталоги батча, перечитывают результат для проверки
   и последним публикуют complete cleaned-манифест.

Сверка: `read = movies + quarantined + duplicates`. Карантин остаётся
в cleaned и не загружается в staging. Невалидные значения необязательных дат
и decimal-полей при безопасном преобразовании могут стать NULL.

Флаги `genres_complete` / `countries_complete` сохраняют различие между
неизвестным набором и явным пустым массивом: массив без элементов с отсутствующим
`id` считается полным, в том числе пустой. NULL или некорректные элементы
не разрешают явно очистить связи. Семантика downstream — в [dbt README](../dbt/README.md).

Перед обработкой старый cleaned-манифест удаляется. Parquet-таблицы
перезаписываются по отдельности: общей транзакции S3 нет. Complete-манифест
служит признаком завершения; конкурентное чтение и перезапись одного батча
не поддерживаются. В Airflow обработка сериализована pool.

## Загрузка staging

`jobs/kinopoisk_cleaned_to_stg.py` использует `kinopoisk_staging.py`,
`kinopoisk_stg_support.py` и `kinopoisk_stg_jdbc.py`.
Загрузчик проверяет complete-манифест, схемы Parquet и SQL, счётчики, batch_id,
обязательные поля, уникальные ключи и связи. Целевые таблицы должны быть
созданы заранее через [DDL staging](../infra/cloudberry/sql/staging.sql).

| Cleaned | Целевая таблица | Ключ |
| --- | --- | --- |
| movies | `stg.movies` | batch_id × id |
| movie_genres | `stg.movie_genres` | batch_id × id × genre_id |
| movie_countries | `stg.movie_countries` | batch_id × id × country_id |

Сначала Spark пишет в отдельные SQL-таблицы `stg._load_<uuid>_*`.
После повторных проверок загрузчик блокирует три целевые таблицы, удаляет
только указанный батч и вставляет его новое содержимое одной транзакцией.
Другие батчи сохраняются; пустой батч удаляет прежние строки этого batch_id.
Ошибка до commit оставляет опубликованные данные неизменными.

Временные загрузочные таблицы видны отдельным JDBC-соединениям и потому
не являются SQL TEMP-таблицами; в штатном завершении они удаляются в `finally`.
После аварийного убийства процесса могут остаться `stg._load_*`: перед ручным
удалением убедитесь, что соответствующий загрузчик уже не работает.
`stg.top250` этот загрузчик не заполняет.

## Ручной запуск

После [инициализации стенда](../infra/README.md) можно запустить обработку
без Airflow. Выполняйте её последовательно и вне работающих DAG.
Замените URI и batch_id значениями существующего манифеста:

```bash
docker compose run --rm spark-submit \
  --master 'local[2]' kinopoisk_raw_to_cleaned.py \
  --manifest s3://etl-raw/kinopoisk/movies/dt=2026-11-11/run=20261112T042321Z/_manifest.json

docker compose run --rm spark-submit \
  --master 'local[2]' kinopoisk_cleaned_to_stg.py \
  --manifest s3://etl-cleaned/kinopoisk/_manifests/batch_id=20261112T042321Z/_manifest.json
```

`local[2]` запускает вычисления внутри временного контейнера. В DAG используется
standalone-кластер. Для raw → cleaned также доступны `--output-bucket`,
`--input-root`, `--output-root`; последние два позволяют подменить корни путей
для локальных проверок. У staging CLI доступен только `--manifest`.

Compose передаёт staging-загрузчику `CLOUDBERRY_HOST=cloudberry`, порт `7000`,
БД `etl`, пользователя `gpadmin` и пароль из `.env`.
Ручные команды выше заканчиваются staging: дальнейшие
[dbt](../dbt/README.md) и [ClickHouse](../infra/clickhouse/README.md)
запускаются отдельно.

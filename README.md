# Лабораторная работа 3 по инженерии данных

Вариант 9, программист, разработчик на мобильных платформах.

Отчет лежит в [output/Лабораторная_работа_3_MapReduce.pdf](output/Лабораторная_работа_3_MapReduce.pdf).

## Что сделано

RustFS 1.0.0 поднят по профилю home из стенда ДЗ 2, четыре папки-диска, схема EC 2+2, бакет hh-lake.

Датасет it_vacancies_2024.csv загружен в объект s3://hh-lake/raw/it_vacancies_2024.csv.

Свой MapReduce в scripts/mapreduce.py читает объект из S3 сплитами по 32 МиБ и считает навыки, пары и тройки навыков варианта. Результаты лежат в s3://hh-lake/results/mapreduce.

Тот же расчет в DuckDB сделан в scripts/duckdb_skills.py, результаты лежат в s3://hh-lake/results/duckdb.

Скрипт scripts/verify.py сравнивает оба результата. Также проверены отказ map-задачи и время с памятью по пять запусков.

Влияние пар и троек навыков на зарплату считает scripts/salary.py, сравнение Москвы, Санкт-Петербурга и Екатеринбурга делает scripts/regions.py.

## Запуск

Нужны Docker, Python 3.12 и файл input/it_vacancies_2024.csv. В файле .env задаются RUSTFS_ACCESS_KEY и RUSTFS_SECRET_KEY.

```sh
python3 -m venv .venv
.venv/bin/pip install boto3 duckdb psutil playwright
mkdir -p data/vol1 data/vol2 data/vol3 data/vol4
docker compose --profile home up -d rustfs-home
.venv/bin/python scripts/upload.py
.venv/bin/python scripts/mapreduce.py --upload
.venv/bin/python scripts/duckdb_skills.py --upload
.venv/bin/python scripts/verify.py
.venv/bin/python scripts/mapreduce.py --fail-map 5 --job failtest --out mapreduce_failtest
.venv/bin/python scripts/combinatorics.py
.venv/bin/python scripts/salary.py
.venv/bin/python scripts/regions.py
.venv/bin/python scripts/profile.py mr_opt 3 -- .venv/bin/python scripts/mapreduce.py --workers 8
```

## Файлы

compose.yaml описывает стенд RustFS.

scripts/common.py содержит подключение к S3, условие варианта и разбор навыков, scripts/sql.py то же условие и расчет зарплаты на SQL.

scripts/test_splits.py проверяет, что каждая строка файла попадает ровно в один сплит. scripts/bench_read.py только читает объект из RustFS. scripts/screenshots.py делает снимки для отчета.

В папке logs лежат выводы запусков, в results результаты расчетов, в screenshots снимки, в report исходник отчета в LaTeX.

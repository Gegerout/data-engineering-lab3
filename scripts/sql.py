"""DuckDB поверх RustFS, отбор вакансий варианта и нормализация зарплаты."""
import duckdb

from common import BUCKET, RAW_KEY, VARIANT_NAME_RE, VARIANT_ROLE, env

RAW = f's3://{BUCKET}/{RAW_KEY}'

SEGMENT_SQL = f"""
CREATE OR REPLACE TABLE seg AS
SELECT vacancy_id, area, name, experience, schedule,
       salary_from, salary_to, currency, gross,
       list_sort(list_distinct(list_filter(
           list_transform(string_split(key_skills, ';'), x -> lower(trim(x))),
           x -> x <> ''))) AS skills
FROM read_csv('{RAW}', header = true, all_varchar = true)
WHERE role = '{VARIANT_ROLE}'
  AND regexp_matches(lower(name), '{VARIANT_NAME_RE}')
"""


# Рубли на руки, середина вилки или одна граница, gross умножается на 0.87,
# суммы меньше 20000 считаются ошибкой ввода.
SALARY_SQL = """
CREATE OR REPLACE TABLE sal AS
SELECT * FROM (
    SELECT seg.*, (CASE WHEN gross = 'true' THEN 0.87 ELSE 1 END) *
           coalesce((try_cast(salary_from AS DOUBLE) + try_cast(salary_to AS DOUBLE)) / 2,
                    try_cast(salary_from AS DOUBLE), try_cast(salary_to AS DOUBLE)) AS salary
    FROM seg WHERE currency = 'RUR')
WHERE salary BETWEEN 20000 AND 1000000
"""

JUNIOR = "experience IN ('Нет опыта', 'От 1 года до 3 лет')"


def connect():
    e = env()
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    con.execute(f"""CREATE SECRET rustfs (TYPE s3, KEY_ID '{e['RUSTFS_ACCESS_KEY']}',
        SECRET '{e['RUSTFS_SECRET_KEY']}', ENDPOINT '127.0.0.1:9010', URL_STYLE 'path',
        USE_SSL false, REGION 'us-east-1')""")
    return con


def show(con, title, sql):
    print('\n' + title)
    rel = con.sql(sql)
    fmt = lambda x: (f'{x:.0f}' if x == int(x) else f'{x:.1f}') if isinstance(x, float) else str(x)
    for r in rel.fetchall():
        print(' '.join(f'{c}={fmt(x)}' for c, x in zip(rel.columns, r)))

"""Частоты навыков, пар и троек варианта в DuckDB по тому же объекту S3."""
import argparse
import sys
import time

from common import BUCKET, LAB
from sql import SEGMENT_SQL, connect

COMBOS_SQL = {
    1: """SELECT skill AS skill_1, count(*) AS vacancies
          FROM ex GROUP BY ALL""",
    2: """SELECT a.skill AS skill_1, b.skill AS skill_2, count(*) AS vacancies
          FROM ex a JOIN ex b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
          GROUP BY ALL""",
    3: """SELECT a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3, count(*) AS vacancies
          FROM ex a JOIN ex b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
                    JOIN ex c ON b.vacancy_id = c.vacancy_id AND b.skill < c.skill
          GROUP BY ALL""",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--upload', action='store_true', help='записать результаты в s3://hh-lake/results/duckdb/')
    a = ap.parse_args()
    print('$ .venv/bin/python scripts/duckdb_skills.py ' + ' '.join(sys.argv[1:]), flush=True)
    t0 = time.perf_counter()
    log = lambda m: print(f'{time.perf_counter() - t0:.2f}s {m}', flush=True)
    con = connect()
    con.execute(SEGMENT_SQL)
    n, ns = con.sql('SELECT count(*), count(*) FILTER (WHERE len(skills) > 0) FROM seg').fetchone()
    log(f'variant_vacancies={n} with_skills={ns}')
    con.execute('CREATE TABLE ex AS SELECT vacancy_id, unnest(skills) AS skill FROM seg')
    out = LAB / 'results' / 'duckdb'
    out.mkdir(parents=True, exist_ok=True)
    for k in (1, 2, 3):
        cols = ', '.join(f'skill_{j + 1}' for j in range(k))
        con.execute(f'CREATE TABLE r{k} AS {COMBOS_SQL[k]} ORDER BY vacancies DESC, {cols}')
        distinct, total = con.sql(f'SELECT count(*), sum(vacancies) FROM r{k}').fetchone()
        log(f'k={k} distinct={distinct} total={total}')
        for i, r in enumerate(con.sql(f'SELECT * FROM r{k} LIMIT 3').fetchall(), 1):
            log(f'k={k} top{i} {" + ".join(r[:-1])} vacancies={r[-1]}')
        con.execute(f"COPY r{k} TO '{out / f'skills_k{k}.csv'}' (HEADER)")
        if a.upload:
            con.execute(f"COPY r{k} TO 's3://{BUCKET}/results/duckdb/skills_k{k}.csv' (HEADER)")
            log(f'uploaded s3://{BUCKET}/results/duckdb/skills_k{k}.csv')
    log('done')


if __name__ == '__main__':
    main()

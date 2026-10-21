"""Рост числа пар и троек, сумма C(n, k) по вакансиям, где n число навыков вакансии."""
from sql import SEGMENT_SQL, connect

print('$ .venv/bin/python scripts/combinatorics.py', flush=True)
con = connect()
con.execute(SEGMENT_SQL)
con.execute('CREATE TABLE n AS SELECT len(skills) AS n FROM seg WHERE len(skills) > 0')
con.execute('CREATE MACRO binomial(n, k) AS CASE k WHEN 1 THEN n WHEN 2 THEN n * (n - 1) // 2 ELSE n * (n - 1) * (n - 2) // 6 END')
print('skills_per_vacancy vacancies=%d min=%d median=%s mean=%.2f max=%d' %
      con.sql('SELECT count(*), min(n), median(n), avg(n), max(n) FROM n').fetchone())
for k in (1, 2, 3):
    total, = con.sql(f'SELECT sum(CAST(binomial(n, {k}) AS BIGINT)) FROM n').fetchone()
    print(f'k={k} combinations={total}')
for row in con.sql('''SELECT n, count(*) v, binomial(n, 2) c2, binomial(n, 3) c3,
                             count(*) * binomial(n, 3) t,
                             round(100 * count(*) * binomial(n, 3) / sum(count(*) * binomial(n, 3)) OVER (), 1)
                      FROM n GROUP BY n ORDER BY n''').fetchall():
    print('n=%d vacancies=%d pairs_each=%d triples_each=%d triples=%d triples_share=%.1f' % row)
share, = con.sql('SELECT round(100 * sum(binomial(n, 3)) FILTER (WHERE n >= 10) / sum(binomial(n, 3)), 1) FROM n').fetchone()
vac, = con.sql('SELECT round(100 * count(*) FILTER (WHERE n >= 10) / count(*), 1) FROM n').fetchone()
print(f'skills_10_or_more vacancies_share={vac} triples_share={share}')
for k in (1, 2, 3):
    d, once = con.sql(f'''SELECT count(*), count(*) FILTER (WHERE vacancies = 1)
                          FROM read_csv('s3://hh-lake/results/mapreduce/skills_k{k}.csv', header = true)''').fetchone()
    print(f'k={k} distinct={d} seen_once={once} seen_once_share={100 * once / d:.1f}')

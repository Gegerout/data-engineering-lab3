"""Сравнение трех регионов по зарплатам, опыту, удаленке, навыкам, парам и тройкам."""
from common import BUCKET, LAB
from sql import JUNIOR, SALARY_SQL, SEGMENT_SQL, connect, show

REGIONS = ['Москва', 'Санкт-Петербург', 'Екатеринбург']
IN = ', '.join(f"'{r}'" for r in REGIONS)


def main():
    print('$ .venv/bin/python scripts/regions.py', flush=True)
    con = connect()
    con.execute(SEGMENT_SQL)
    con.execute(SALARY_SQL)
    show(con, 'regions by vacancies with salary', '''
        SELECT seg.area, count(*) AS vacancies, count(sal.salary) AS with_salary
        FROM seg LEFT JOIN sal USING (vacancy_id) GROUP BY seg.area ORDER BY with_salary DESC, vacancies DESC LIMIT 6''')
    con.execute(f'CREATE TABLE r AS SELECT seg.*, sal.salary FROM seg LEFT JOIN sal USING (vacancy_id) WHERE seg.area IN ({IN})')
    show(con, 'table 1 key indicators', f'''
        SELECT area, count(*) AS vacancies,
               round(100 * count(*) / (SELECT count(*) FROM seg)) AS pct_of_variant,
               count(salary) AS with_salary, quantile_cont(salary, 0.25) AS q25,
               median(salary) AS median_salary, quantile_cont(salary, 0.75) AS q75,
               round(100 * avg(CASE WHEN {JUNIOR} THEN 1 ELSE 0 END)) AS junior_pct,
               round(100 * avg(CASE WHEN schedule = 'Удаленная работа' THEN 1 ELSE 0 END)) AS remote_pct,
               count(*) FILTER (WHERE len(skills) > 0) AS with_skills,
               round(avg(len(skills)) FILTER (WHERE len(skills) > 0), 1) AS avg_skills
        FROM r GROUP BY area ORDER BY vacancies DESC''')
    show(con, 'median salary by experience', f'''
        SELECT area, experience, count(salary) AS n, median(salary) AS median_salary
        FROM r GROUP BY ALL HAVING count(salary) > 0 ORDER BY area, median_salary''')
    con.execute('CREATE TABLE e AS SELECT vacancy_id, area, salary, unnest(skills) AS skill FROM r')
    con.execute('''CREATE TABLE c AS
        SELECT area, 1 AS k, skill AS combo, count(*) AS vacancies, count(salary) AS n_salary, median(salary) AS med
        FROM e GROUP BY ALL
        UNION ALL
        SELECT a.area, 2, a.skill || ' + ' || b.skill, count(*), count(a.salary), median(a.salary)
        FROM e a JOIN e b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill GROUP BY ALL
        UNION ALL
        SELECT a.area, 3, a.skill || ' + ' || b.skill || ' + ' || x.skill, count(*), count(a.salary), median(a.salary)
        FROM e a JOIN e b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
                 JOIN e x ON b.vacancy_id = x.vacancy_id AND b.skill < x.skill GROUP BY ALL''')
    con.execute('''CREATE TABLE ws AS SELECT area, count(*) AS n FROM r WHERE len(skills) > 0 GROUP BY area''')
    con.execute('''CREATE TABLE t2 AS
        SELECT c.area, k, row_number() OVER (PARTITION BY c.area, k ORDER BY vacancies DESC, combo) AS rank,
               combo, vacancies, round(100 * vacancies / ws.n) AS pct_with_skills, n_salary, med AS median_salary
        FROM c JOIN ws USING (area)''')
    show(con, 'table 2 top 5 skills 3 pairs 3 triples', '''
        SELECT * FROM t2 WHERE (k = 1 AND rank <= 5) OR (k > 1 AND rank <= 3) ORDER BY area, k, rank''')
    show(con, 'skill share by region for top 12 skills', f'''
        WITH top AS (SELECT skill FROM (SELECT unnest(skills) AS skill FROM seg)
                     GROUP BY skill ORDER BY count(*) DESC, skill LIMIT 12)
        SELECT skill, {', '.join(f"round(100 * count(*) FILTER (WHERE area = '{x}') / (SELECT n FROM ws WHERE area = '{x}')) AS \"{x}\"" for x in REGIONS)}
        FROM e JOIN top USING (skill) GROUP BY skill ORDER BY count(*) DESC''')
    out = LAB / 'results' / 'analysis' / 'regions_top.csv'
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * FROM t2 WHERE rank <= 20 ORDER BY area, k, rank) TO '{out}' (HEADER)")
    con.execute(f"COPY (SELECT * FROM t2 WHERE rank <= 20 ORDER BY area, k, rank) TO 's3://{BUCKET}/results/analysis/regions_top.csv' (HEADER)")
    print(f'\nuploaded s3://{BUCKET}/results/analysis/regions_top.csv')


if __name__ == '__main__':
    main()

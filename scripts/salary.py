"""Влияние пар и троек навыков на медианную зарплату варианта."""
from common import BUCKET, LAB
from sql import JUNIOR, SALARY_SQL, SEGMENT_SQL, connect, show

MIN_N = 15  # минимум вакансий с зарплатой у сочетания
TOP = 10


def main():
    print('$ .venv/bin/python scripts/salary.py', flush=True)
    con = connect()
    con.execute(SEGMENT_SQL)
    con.execute(SALARY_SQL)
    show(con, 'funnel', f'''SELECT
        (SELECT count(*) FROM seg) AS variant,
        (SELECT count(*) FROM seg WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL) AS any_salary,
        (SELECT count(*) FROM seg WHERE currency = 'RUR') AS rur,
        (SELECT count(*) FROM sal) AS rur_valid,
        (SELECT count(*) FROM sal WHERE len(skills) > 0) AS rur_valid_with_skills,
        (SELECT median(salary) FROM sal WHERE len(skills) > 0) AS median_salary,
        (SELECT round(100 * avg(CASE WHEN {JUNIOR} THEN 1 ELSE 0 END)) FROM sal WHERE len(skills) > 0) AS junior_pct''')

    con.execute('CREATE TABLE s AS SELECT * FROM sal WHERE len(skills) > 0')
    con.execute('CREATE TABLE q AS SELECT *, ntile(4) OVER (ORDER BY salary, vacancy_id) AS quartile FROM s')
    show(con, 'salary quartiles', f'''
        WITH top AS (
            SELECT quartile, skill, count(*) AS c,
                   row_number() OVER (PARTITION BY quartile ORDER BY count(*) DESC, skill) AS rn
            FROM (SELECT quartile, unnest(skills) AS skill FROM q) GROUP BY quartile, skill),
        st AS (
            SELECT quartile, count(*) AS n, min(salary) AS min_salary, median(salary) AS median_salary,
                   max(salary) AS max_salary, round(avg(len(skills)), 1) AS avg_skills,
                   round(100 * avg(CASE WHEN {JUNIOR} THEN 1 ELSE 0 END)) AS junior_pct
            FROM q GROUP BY quartile)
        SELECT st.*, string_agg(skill || ' ' || c, ', ' ORDER BY rn) AS top_skills
        FROM st JOIN top ON top.quartile = st.quartile AND rn <= 5
        GROUP BY ALL ORDER BY st.quartile''')

    con.execute(f'''CREATE TABLE f1 AS SELECT skill_1 AS skill, vacancies AS freq
                    FROM read_csv('s3://{BUCKET}/results/mapreduce/skills_k1.csv', header = true)''')
    con.execute(f'''CREATE TABLE f2 AS SELECT skill_1 AS a, skill_2 AS b, vacancies AS freq
                    FROM read_csv('s3://{BUCKET}/results/mapreduce/skills_k2.csv', header = true)''')
    con.execute(f'CREATE TABLE ex AS SELECT vacancy_id, salary, {JUNIOR} AS junior, unnest(skills) AS skill FROM s')
    con.execute('CREATE TABLE m1 AS SELECT skill, count(*) AS n, median(salary) AS med FROM ex GROUP BY skill')
    con.execute('''CREATE TABLE m2 AS
        SELECT a.skill AS s1, b.skill AS s2, count(*) AS n, median(a.salary) AS med,
               round(100 * avg(a.junior::INT)) AS junior_pct
        FROM ex a JOIN ex b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill GROUP BY ALL''')
    con.execute('''CREATE TABLE m3 AS
        SELECT a.skill AS s1, b.skill AS s2, c.skill AS s3, count(*) AS n, median(a.salary) AS med,
               round(100 * avg(a.junior::INT)) AS junior_pct
        FROM ex a JOIN ex b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
                  JOIN ex c ON b.vacancy_id = c.vacancy_id AND b.skill < c.skill GROUP BY ALL''')

    # база пары это более частый навык по результатам MapReduce
    con.execute(f'''CREATE TABLE up2 AS
        WITH b AS (
            SELECT m2.*, CASE WHEN fa.freq > fb.freq OR (fa.freq = fb.freq AND s1 < s2) THEN s1 ELSE s2 END AS base
            FROM m2 JOIN f1 fa ON fa.skill = s1 JOIN f1 fb ON fb.skill = s2 WHERE m2.n >= {MIN_N})
        SELECT s1 || ' + ' || s2 AS combo, b.n, base, m1.n AS base_n, m1.med AS base_med,
               CASE WHEN base = s1 THEN s2 ELSE s1 END AS added, b.med,
               b.med - m1.med AS uplift, round(100 * (b.med / m1.med - 1)) AS uplift_pct, b.junior_pct
        FROM b JOIN m1 ON m1.skill = base''')
    # база тройки это самая частая из трех вложенных пар
    con.execute(f'''CREATE TABLE up3 AS
        WITH cand AS (
            SELECT t.*, x.a, x.b, x.added, f2.freq,
                   row_number() OVER (PARTITION BY s1, s2, s3 ORDER BY f2.freq DESC, x.a, x.b) AS rn
            FROM m3 t,
                 (SELECT unnest([{{'a': t.s1, 'b': t.s2, 'added': t.s3}},
                                 {{'a': t.s1, 'b': t.s3, 'added': t.s2}},
                                 {{'a': t.s2, 'b': t.s3, 'added': t.s1}}], recursive := true)) x
                 JOIN f2 ON f2.a = x.a AND f2.b = x.b
            WHERE t.n >= {MIN_N})
        SELECT cand.s1 || ' + ' || cand.s2 || ' + ' || cand.s3 AS combo, cand.n, cand.a || ' + ' || cand.b AS base,
               m2.n AS base_n, m2.med AS base_med, added, cand.med,
               cand.med - m2.med AS uplift, round(100 * (cand.med / m2.med - 1)) AS uplift_pct, cand.junior_pct
        FROM cand JOIN m2 ON m2.s1 = cand.a AND m2.s2 = cand.b WHERE rn = 1''')
    for k in (2, 3):
        show(con, f'k={k} raise median min_n={MIN_N}',
             f'SELECT * FROM up{k} ORDER BY uplift DESC, combo LIMIT {TOP}')
        show(con, f'k={k} lower median min_n={MIN_N}',
             f'SELECT * FROM up{k} ORDER BY uplift ASC, combo LIMIT {TOP}')
        out = LAB / 'results' / 'analysis' / f'salary_uplift_k{k}.csv'
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY (SELECT * FROM up{k} ORDER BY uplift DESC, combo) TO '{out}' (HEADER)")
        con.execute(f"COPY (SELECT * FROM up{k} ORDER BY uplift DESC, combo) "
                    f"TO 's3://{BUCKET}/results/analysis/salary_uplift_k{k}.csv' (HEADER)")
        print(f'uploaded s3://{BUCKET}/results/analysis/salary_uplift_k{k}.csv rows={con.sql(f"SELECT count(*) FROM up{k}").fetchone()[0]}')


if __name__ == '__main__':
    main()

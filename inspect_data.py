import json
from pathlib import Path
from jobs import duckdb_connection, load_config, sql_literal

cfg = load_config('config.json')
db = duckdb_connection('.duckdb_extensions')
source = f"read_parquet({sql_literal('s3://'+cfg['bucket']+'/optimized/it_vacancies_core.parquet')})"
sql = f"""CREATE TEMP TABLE source_variant AS
SELECT *, list_sort(list_distinct(list_filter(list_transform(
    string_split(coalesce(key_skills, ''), ';'),
    x -> lower(trim(x, chr(32)||chr(9)||chr(10)||chr(13)))), x->x<>''))) skills
FROM {source} WHERE {cfg['sql_where']};
"""
db.execute(sql)

first = Path('results/reference-sql.sql').read_text().split(';\n\n')[0]
start = first.index('read_csv(')
end = first.index(' WHERE ', start)
first = first[:start]+source+first[end:]
db.execute(first)
result = dict(
    overall=db.execute('SELECT count(*),count(salary2),median(salary2)/2 FROM selected').fetchone(),
    currency_gross=db.execute('SELECT currency,gross,count(*) FROM source_variant GROUP BY ALL ORDER BY 3 DESC').fetchall(),
    regional_coverage=db.execute('SELECT area,count(*),count(*) FILTER(WHERE len(skills)=0),count(salary2),median(salary2)/2 FROM selected GROUP BY area ORDER BY count(*) DESC').fetchall())
result['lower_salary_examples'] = db.execute("""
SELECT name,area,skills,salary2/2 FROM source_variant
JOIN selected USING(vacancy_id,area,skills)
WHERE list_contains(skills,'java') AND salary2 IS NOT NULL AND
 (list_contains(skills,'обучение и развитие') OR
 (list_contains(skills,'c#') AND list_contains(skills,'sql')))
ORDER BY salary2,name LIMIT 15
""").fetchall()
Path('results/coverage.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
Path('results/coverage.sql').write_text(sql+'\n'+first+';\n')
print(json.dumps(result,ensure_ascii=False,indent=2))

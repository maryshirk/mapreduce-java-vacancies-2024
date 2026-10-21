import argparse
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
import csv
import hashlib
import io
import itertools
import json
import os
from pathlib import Path
import re
import statistics
import time


FIELDS = ('vacancy_id', 'name', 'role', 'area', 'salary_from', 'salary_to',
          'currency', 'gross', 'key_skills')
STRIP = ' \t\r\n'


def load_config(path):
    return json.loads(Path(path).read_text())


def s3_client():
    import boto3
    from botocore.config import Config
    credentials_path = os.environ.get('LAB_CREDENTIALS_FILE')
    credentials = json.loads(Path(credentials_path).read_text()) if credentials_path else {}
    return boto3.client(
        's3', endpoint_url=os.environ.get('LAB_S3_ENDPOINT', 'http://127.0.0.1:9000'),
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', credentials.get('access_key')),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', credentials.get('secret_key')),
        region_name='us-east-1',
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                      retries={'total_max_attempts': 2}, max_pool_connections=12,
                      request_checksum_calculation='when_required',
                      response_checksum_validation='when_required'),
    )


class StreamingReader(io.RawIOBase):


    def __init__(self, body):
        self.body = body
        self.byte_count = 0
        self.digest = hashlib.sha256()

    def readable(self):
        return True

    def readinto(self, buffer):
        data = self.body.read(len(buffer))
        buffer[:len(data)] = data
        self.byte_count += len(data)
        self.digest.update(data)
        return len(data)

    def close(self):
        self.body.close()
        super().close()


def parse_skills(value):
    return tuple(sorted({skill.strip(STRIP).lower() for skill in value.split(';')
                         if skill.strip(STRIP)}))


def salary_twice(row):

    if row['currency'] != 'RUR' or row['gross'].lower() != 'false':
        return None
    values = []
    for name in ('salary_from', 'salary_to'):
        text = row[name].strip(STRIP)
        if re.fullmatch(r'[0-9]+(?:\.0+)?', text):
            value = int(text.split('.')[0])
            if value > 0:
                values.append(value)
    if len(values) == 2:
        return sum(values)
    return 2 * values[0] if values else None


def selected(row, config):
    title_ok = bool(re.search(config['title_regex'], row['name'], re.IGNORECASE))
    skills_ok = bool(set(config.get('skill_any', ())) & set(parse_skills(row['key_skills'])))
    roles = config.get('roles')
    return (title_ok or skills_ok) and (not roles or row['role'] in roles)


def mapper(chunk_id, rows, config, attempt=0, fault_chunk=None):
    if chunk_id == fault_chunk and attempt == 0:
        raise RuntimeError('INJECTED_MAPPER_FAILURE: partial result was not committed')
    aggregates = {}
    identifiers = []
    region_values = defaultdict(list)
    region_counts = defaultdict(int)
    skill_histogram = defaultdict(int)
    empty_skills = 0
    for row in rows:
        if not selected(row, config):
            continue
        identifiers.append(row['vacancy_id'])
        skills = parse_skills(row['key_skills'])
        skill_histogram[len(skills)] += 1
        empty_skills += not bool(skills)
        salary = salary_twice(row)
        region_counts[row['area']] += 1
        if salary is not None:
            region_values[row['area']].append(salary)
        scopes = ['__ALL__']
        if row['area'] in config['regions']:
            scopes.append(row['area'])
        for size in (1, 2, 3):
            for combination in itertools.combinations(skills, size):
                for scope in scopes:
                    key = (scope, combination)
                    if key not in aggregates:
                        aggregates[key] = [0, []]
                    aggregates[key][0] += 1
                    if salary is not None:
                        aggregates[key][1].append(salary)
    return dict(chunk_id=chunk_id, aggregates=aggregates, ids=identifiers,
                empty_skills=empty_skills, region_counts=dict(region_counts),
                region_values=dict(region_values), skill_histogram=dict(skill_histogram))


def run_mapreduce(config, *, chunk_size=5000, workers=1, fault_chunk=None):
    started = time.perf_counter()
    source = s3_client().get_object(Bucket=config['bucket'], Key=config['key'])
    raw = StreamingReader(source['Body'])
    text = io.TextIOWrapper(io.BufferedReader(raw, buffer_size=1024 * 1024),
                            encoding='utf-8-sig', newline='')
    csv.field_size_limit(16 * 1024 * 1024)
    reader = csv.DictReader(text)
    aggregates = {}
    ids = []
    region_counts = defaultdict(int)
    region_values = defaultdict(list)
    committed = set()
    failures = 0
    empty_skills = 0
    input_rows = 0
    skill_histogram = defaultdict(int)

    def merge(part):
        nonlocal empty_skills
        if part['chunk_id'] in committed:
            raise RuntimeError('Duplicate reducer commit')
        committed.add(part['chunk_id'])
        ids.extend(part['ids'])
        empty_skills += part['empty_skills']
        for size, count in part['skill_histogram'].items():
            skill_histogram[size] += count
        for key, (count, salaries) in part['aggregates'].items():
            if key not in aggregates:
                aggregates[key] = [0, []]
            aggregates[key][0] += count
            aggregates[key][1].extend(salaries)
        for region, count in part['region_counts'].items():
            region_counts[region] += count
        for region, salaries in part['region_values'].items():
            region_values[region].extend(salaries)

    def chunks():
        nonlocal input_rows
        chunk = []
        chunk_id = 0
        for record in reader:
            if None in record or any(record.get(key) is None for key in FIELDS):
                raise ValueError('Malformed CSV record')
            input_rows += 1
            chunk.append({field: record[field] for field in FIELDS})
            if len(chunk) == chunk_size:
                yield chunk_id, chunk
                chunk_id += 1
                chunk = []
        if chunk:
            yield chunk_id, chunk

    with text:
        if workers == 1:
            for chunk_id, chunk in chunks():
                try:
                    part = mapper(chunk_id, chunk, config, 0, fault_chunk)
                except RuntimeError as error:
                    if not str(error).startswith('INJECTED_MAPPER_FAILURE:'):
                        raise
                    failures += 1
                    part = mapper(chunk_id, chunk, config, 1, fault_chunk)
                merge(part)
        else:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                pending = {}
                source_chunks = iter(chunks())
                exhausted = False
                while pending or not exhausted:
                    while not exhausted and len(pending) < workers * 2:
                        item = next(source_chunks, None)
                        if item is None:
                            exhausted = True
                            break
                        chunk_id, chunk = item
                        future = executor.submit(mapper, chunk_id, chunk, config, 0, fault_chunk)
                        pending[future] = (chunk_id, chunk)
                    if not pending:
                        break
                    complete, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in complete:
                        chunk_id, chunk = pending.pop(future)
                        try:
                            merge(future.result())
                        except RuntimeError as error:
                            if not str(error).startswith('INJECTED_MAPPER_FAILURE:'):
                                raise
                            failures += 1
                            retry = executor.submit(mapper, chunk_id, chunk, config, 1, fault_chunk)
                            pending[retry] = (chunk_id, chunk)

    result = []
    for (scope, skills), (count, salaries) in aggregates.items():
        median = statistics.median(salaries) / 2 if salaries else None
        result.append([scope, list(skills), count, len(salaries), median])
    regions = [[name, region_counts[name], len(region_values[name]),
                statistics.median(region_values[name]) / 2 if region_values[name] else None]
               for name in sorted(region_counts)]
    return dict(records=sorted(result), ids=sorted(ids), regions=regions,
                empty_skills=empty_skills, input_rows=input_rows,
                input_bytes=raw.byte_count, input_sha256=raw.digest.hexdigest(),
                mapper_failures=failures, committed_chunks=len(committed),
                skill_histogram=dict(skill_histogram),
                elapsed_seconds=time.perf_counter()-started)


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def run_pandas(config):

    import pandas as pd
    started = time.perf_counter()
    body = s3_client().get_object(Bucket=config['bucket'], Key=config['key'])['Body']
    with body:
        frame = pd.read_csv(body, dtype=str, keep_default_na=False)
    input_rows = len(frame)
    frame = frame.loc[frame.apply(lambda row: selected(row, config), axis=1), list(FIELDS)].copy()
    frame['skills'] = frame['key_skills'].map(parse_skills)
    frame['salary2'] = frame.apply(salary_twice, axis=1)
    identifiers = sorted(frame['vacancy_id'].tolist())
    histogram = {int(key): int(value) for key, value in frame['skills'].map(len).value_counts().items()}
    regions = []
    for area, group in frame.groupby('area', sort=True):
        salaries = group['salary2'].dropna()
        regions.append([str(area), len(group), len(salaries),
                        float(salaries.median()/2) if len(salaries) else None])
    expanded = []
    for row in frame.itertuples(index=False):
        scopes = ['__ALL__'] + ([row.area] if row.area in config['regions'] else [])
        for size in (1, 2, 3):
            for combination in itertools.combinations(row.skills, size):
                for scope in scopes:
                    expanded.append((scope, combination, row.salary2))
    long = pd.DataFrame(expanded, columns=['scope', 'combination', 'salary2'])
    grouped = long.groupby(['scope', 'combination'], sort=True)['salary2'].agg(['size', 'count', 'median'])
    result = [[scope, list(skills), int(record['size']), int(record['count']),
               None if pd.isna(record['median']) else float(record['median']/2)]
              for (scope, skills), record in grouped.iterrows()]
    return dict(records=sorted(result), ids=identifiers, regions=regions,
                empty_skills=int(histogram.get(0, 0)), skill_histogram=histogram,
                input_rows=input_rows, elapsed_seconds=time.perf_counter()-started)


def duckdb_connection(extension_dir, *, threads=4):
    import duckdb
    extension_dir = Path(extension_dir).resolve()
    extension_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(config={'threads': threads, 'memory_limit': '2GB',
                                         'extension_directory': str(extension_dir)})
    connection.execute('LOAD httpfs')
    path = os.environ.get('LAB_CREDENTIALS_FILE')
    credentials = json.loads(Path(path).read_text()) if path else {}
    key = os.environ.get('AWS_ACCESS_KEY_ID', credentials.get('access_key'))
    secret = os.environ.get('AWS_SECRET_ACCESS_KEY', credentials.get('secret_key'))
    endpoint = os.environ.get('LAB_S3_ENDPOINT', 'http://127.0.0.1:9000')
    use_ssl = endpoint.startswith('https://')
    endpoint = endpoint.removeprefix('https://').removeprefix('http://')
    settings = ', '.join(['TYPE S3', 'PROVIDER CONFIG',
                          'KEY_ID ' + sql_literal(key), 'SECRET ' + sql_literal(secret),
                          'REGION \'us-east-1\'', 'ENDPOINT ' + sql_literal(endpoint),
                          "URL_STYLE 'path'", 'USE_SSL ' + str(use_ssl).lower()])
    try:
        connection.execute('CREATE SECRET lab_s3 (' + settings + ')')
    except duckdb.Error:
        raise RuntimeError('Не удалось настроить S3; проверьте локальные ключи и endpoint.') from None
    return connection


def run_duckdb(config, extension_dir, *, threads=4, parquet_key=None, sql_log=None):
    started = time.perf_counter()
    connection = duckdb_connection(extension_dir, threads=threads)
    key = parquet_key or config['key']
    uri = sql_literal(f"s3://{config['bucket']}/{key}")
    source = f'read_parquet({uri})' if parquet_key else (
        f"read_csv({uri}, header=true, delim=',', all_varchar=true, "
        "max_line_size=16777216, quote='\"', escape='\"')")
    trim_chars = sql_literal(STRIP)
    query = f"""
    CREATE TEMP TABLE selected AS
    WITH filtered AS (
        SELECT * FROM {source} WHERE {config['sql_where']}
    ), parsed AS (
        SELECT *,
            list_sort(list_distinct(list_filter(
                list_transform(string_split(coalesce(key_skills, ''), ';'),
                    x -> lower(trim(x, {trim_chars}))), x -> x <> ''))) AS skills,
            CASE WHEN regexp_full_match(trim(coalesce(salary_from, ''), {trim_chars}),
                        '[0-9]+([.]0+)?')
                 THEN try_cast(salary_from AS BIGINT) END AS lo,
            CASE WHEN regexp_full_match(trim(coalesce(salary_to, ''), {trim_chars}),
                        '[0-9]+([.]0+)?')
                 THEN try_cast(salary_to AS BIGINT) END AS hi
        FROM filtered
    )
    SELECT vacancy_id, coalesce(area, '') AS area, skills,
        CASE WHEN currency = 'RUR' AND lower(gross) = 'false' THEN
            CASE WHEN lo > 0 AND hi > 0 THEN lo + hi
                 WHEN lo > 0 THEN 2*lo WHEN hi > 0 THEN 2*hi END
        END AS salary2
    FROM parsed
    """
    connection.execute(query)
    ids = [row[0] for row in connection.execute('SELECT vacancy_id FROM selected ORDER BY vacancy_id').fetchall()]
    empty = connection.execute('SELECT count(*) FROM selected WHERE len(skills)=0').fetchone()[0]
    skill_histogram = dict(connection.execute(
        'SELECT len(skills), count(*) FROM selected GROUP BY len(skills)').fetchall())
    regions = [list(row) for row in connection.execute(
        'SELECT area, count(*), count(salary2), median(salary2)/2 '
        'FROM selected GROUP BY area ORDER BY area').fetchall()]
    selected_regions = ','.join(sql_literal(region) for region in config['regions'])
    scope_query = ("CREATE TEMP VIEW scoped AS "
        "SELECT '__ALL__' AS scope, skills, salary2 FROM selected UNION ALL "
        f"SELECT area AS scope, skills, salary2 FROM selected WHERE area IN ({selected_regions})")
    connection.execute(scope_query)
    queries = [query, scope_query]
    records = []
    for size in (1, 2, 3):
        indices = ['i', 'j', 'k'][:size]
        joined = 'UNNEST(range(1, len(skills)+1)) a(i)'
        if size >= 2:
            joined += ', UNNEST(range(i+1, len(skills)+1)) b(j)'
        if size >= 3:
            joined += ', UNNEST(range(j+1, len(skills)+1)) c(k)'
        skills = '[' + ','.join(f'skills[{index}]' for index in indices) + ']'
        aggregate = (f'SELECT scope, {skills} AS combination, count(*), '
                     f'count(salary2), median(salary2)/2 FROM scoped, {joined} '
                     'GROUP BY scope, combination')
        queries.append(aggregate)
        records.extend([list(row) for row in connection.execute(aggregate).fetchall()])
    connection.close()
    if sql_log:
        Path(sql_log).write_text(';\n\n'.join(queries) + ';\n')
    return dict(records=sorted(records), ids=ids, regions=regions,
                empty_skills=empty, skill_histogram=skill_histogram,
                elapsed_seconds=time.perf_counter()-started)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('method', choices=['mapreduce', 'duckdb', 'pandas'])
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--chunk-size', type=int, default=5000)
    parser.add_argument('--fault-chunk', type=int)
    parser.add_argument('--extensions', default='.duckdb_extensions')
    parser.add_argument('--parquet-key')
    options = parser.parse_args()
    config = load_config(options.config)
    output = Path(options.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if options.method == 'mapreduce':
        result = run_mapreduce(config, chunk_size=options.chunk_size,
                               workers=options.workers, fault_chunk=options.fault_chunk)
    elif options.method == 'pandas':
        result = run_pandas(config)
    else:
        result = run_duckdb(config, options.extensions, threads=options.workers,
                           parquet_key=options.parquet_key, sql_log=output.with_suffix('.sql'))
    output.write_text(json.dumps(result, ensure_ascii=False, separators=(',', ':')))
    print(json.dumps({key: value for key, value in result.items()
                      if key not in ('records', 'ids', 'regions')}, ensure_ascii=False))
    print('matched_vacancies:', len(result['ids']), 'combination_groups:', len(result['records']))


if __name__ == '__main__':
    main()

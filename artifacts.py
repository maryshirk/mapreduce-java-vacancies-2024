import argparse
import hashlib
import json
from pathlib import Path
import time

from jobs import FIELDS, duckdb_connection, load_config, s3_client, sql_literal


PARQUET_KEY = 'optimized/it_vacancies_core.parquet'


def optimize(config, output):
    started = time.perf_counter()
    client = s3_client()
    connection = duckdb_connection('.duckdb_extensions')
    source = sql_literal(f"s3://{config['bucket']}/{config['key']}")
    output = Path(output)
    output.parent.mkdir(exist_ok=True, parents=True)
    projected = output.parent / 'it_vacancies_core.parquet'
    query = (f"COPY (SELECT {', '.join(FIELDS)} FROM read_csv({source}, "
             "header=true, delim=',', all_varchar=true, max_line_size=16777216)) "
             f"TO {sql_literal(projected.resolve())} (FORMAT PARQUET, COMPRESSION ZSTD)")
    connection.execute(query)
    rows = connection.execute(f'SELECT count(*) FROM read_parquet({sql_literal(projected.resolve())})').fetchone()[0]
    connection.close()
    with projected.open('rb') as stream:
        client.put_object(Bucket=config['bucket'], Key=PARQUET_KEY, Body=stream,
                          ContentLength=projected.stat().st_size, StorageClass='STANDARD',
                          ContentType='application/vnd.apache.parquet')
    result = dict(key=PARQUET_KEY, rows=rows, bytes=projected.stat().st_size,
                  elapsed_seconds=time.perf_counter()-started, columns=list(FIELDS),
                  sha256=hashlib.sha256(projected.read_bytes()).hexdigest())
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps(result, ensure_ascii=False))


def store(config, result_path, output):
    import pyarrow as pa
    import pyarrow.parquet as pq
    result = json.loads(Path(result_path).read_text())
    client = s3_client()
    output = Path(output)
    output.mkdir(exist_ok=True, parents=True)
    objects = []
    for size in (1, 2, 3):
        records = [dict(region=row[0], skills=row[1], frequency=row[2],
                        salary_count=row[3], median_salary_rub=row[4])
                   for row in result['records'] if len(row[1]) == size]
        path = output/f'java_skills_{size}.parquet'
        pq.write_table(pa.Table.from_pylist(records), path, compression='zstd')
        key = 'results/java/' + path.name
        with path.open('rb') as stream:
            response = client.put_object(Bucket=config['bucket'], Key=key, Body=stream,
                                         ContentLength=path.stat().st_size, StorageClass='STANDARD')
        body = client.get_object(Bucket=config['bucket'], Key=key)['Body']
        with body:
            remote_hash = hashlib.sha256(body.read()).hexdigest()
        local_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        assert remote_hash == local_hash
        objects.append(dict(key=key, groups=len(records), bytes=path.stat().st_size,
                            sha256=local_hash, http=response['ResponseMetadata']['HTTPStatusCode']))
    (output/'s3-results.json').write_text(json.dumps(objects, ensure_ascii=False, indent=2))
    print(json.dumps(objects, ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['optimize', 'store'])
    parser.add_argument('--config', default='config.json')
    parser.add_argument('--result', default='results/reference-mr.json')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    settings = load_config(args.config)
    if args.action == 'optimize':
        optimize(settings, args.output)
    else:
        store(settings, args.result, args.output)

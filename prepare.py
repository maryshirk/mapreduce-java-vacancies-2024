import argparse
import hashlib
import json
from pathlib import Path

from jobs import s3_client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--from-bucket')
    parser.add_argument('--source-file')
    parser.add_argument('--bucket', default='hh-lake')
    parser.add_argument('--key', default='raw/it_vacancies_2024.csv')
    parser.add_argument('--output', required=True)
    options = parser.parse_args()
    if bool(options.from_bucket) == bool(options.source_file):
        parser.error('Choose --from-bucket or --source-file')
    client = s3_client()
    from botocore.exceptions import ClientError
    try:
        client.head_bucket(Bucket=options.bucket)
    except ClientError as error:
        if error.response['Error']['Code'] not in ('404', 'NoSuchBucket', 'NotFound'):
            raise
        client.create_bucket(Bucket=options.bucket)
    if options.from_bucket:
        response = client.copy_object(
            Bucket=options.bucket, Key=options.key,
            CopySource={'Bucket': options.from_bucket, 'Key': 'it_vacancies_2024.csv'},
            StorageClass='STANDARD')
    else:
        path = Path(options.source_file)
        with path.open('rb') as stream:
            response = client.put_object(
                Bucket=options.bucket, Key=options.key, Body=stream,
                ContentLength=path.stat().st_size, ContentType='text/csv',
                StorageClass='STANDARD')
    downloaded = client.get_object(Bucket=options.bucket, Key=options.key)
    digest = hashlib.sha256()
    size = 0
    with downloaded['Body'] as body:
        for block in iter(lambda: body.read(1024 * 1024), b''):
            digest.update(block)
            size += len(block)
    result = {'bucket': options.bucket, 'key': options.key,
              'http': response['ResponseMetadata']['HTTPStatusCode'],
              'bytes': size, 'sha256': digest.hexdigest()}
    output = Path(options.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()

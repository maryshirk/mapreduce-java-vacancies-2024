import csv
import gc
import hashlib
import itertools
import json
import math
from pathlib import Path
import statistics


CHECK_FIELDS = ('records', 'ids', 'regions', 'empty_skills', 'skill_histogram')


def signature(result):
    comparable = {key: result[key] for key in CHECK_FIELDS}
    return hashlib.sha256(json.dumps(comparable, ensure_ascii=False, sort_keys=True,
                                     separators=(',', ':')).encode()).hexdigest()


def main():
    root = Path('results')
    root.mkdir(exist_ok=True)
    ref = json.loads((root/'reference-mr.json').read_text())
    expected = signature(ref)
    paths = [root/'reference-sql.json', root/'fault-retry.json',
             *sorted((root/'benchmark').glob('*.json'))]
    checks = []
    for path in paths:
        actual = json.loads(path.read_text())
        differences = [key for key in CHECK_FIELDS if ref[key] != actual[key]]
        assert not differences, (str(path), differences)
        assert signature(actual) == expected
        checks.append(dict(file=str(path), full_equality=True, digest=expected))
        del actual
        gc.collect()
    assert len(checks) == 17, len(checks)
    verification = dict(fields=CHECK_FIELDS, groups=len(ref['records']),
                        matched=len(ref['ids']), digest=expected, checks=checks)
    (root/'verification.json').write_text(json.dumps(verification, ensure_ascii=False, indent=2))
    index = {(row[0], tuple(row[1])): row for row in ref['records']}
    global_rows = [row for row in ref['records'] if row[0] == '__ALL__']
    summary = dict(matched=len(ref['ids']), input_rows=ref['input_rows'],
                   input_bytes=ref['input_bytes'], empty_skills=ref['empty_skills'],
                   salary_count=sum(row[2] for row in ref['regions']),
                   salary_regions=ref['regions'])
    summary['combinations'] = []
    for size in (1, 2, 3):
        rows = [row for row in global_rows if len(row[1]) == size]
        occurrences = sum(row[2] for row in rows)
        invariant = sum(math.comb(int(m), size)*count for m, count in ref['skill_histogram'].items()
                        if int(m) >= size)
        assert occurrences == invariant
        summary['combinations'].append(dict(size=size, unique=len(rows),
                                            occurrences=occurrences, histogram_sum=invariant))
    summary['top_skills'] = sorted([row for row in global_rows if len(row[1]) == 1],
                                    key=lambda r: (-r[2], r[1]))[:15]
    summary['salary_associations'] = {}
    for size in (2, 3):
        associations = []
        for row in global_rows:
            if len(row[1]) != size or row[3] < 20 or row[4] is None:
                continue
            candidates = [index['__ALL__', base] for base in itertools.combinations(row[1], size-1)]
            baseline = sorted(candidates, key=lambda r: (-r[2], r[1]))[0]
            assert baseline[3] >= row[3]
            added = sorted(set(row[1])-set(baseline[1]))
            delta = row[4]-baseline[4]
            associations.append(dict(skills=row[1], frequency=row[2], salary_n=row[3],
                median=row[4], base=baseline[1], base_frequency=baseline[2],
                base_salary_n=baseline[3], base_median=baseline[4], added=added,
                delta=delta, percent=100*delta/baseline[4]))
        positive = sorted([row for row in associations if row['delta'] > 0],
                          key=lambda r: (-r['percent'], -r['salary_n'], r['skills']))
        negative = sorted([row for row in associations if row['delta'] < 0],
                          key=lambda r: (r['percent'], -r['salary_n'], r['skills']))
        summary['salary_associations'][str(size)] = dict(eligible=len(associations),
                                                         increases=positive[:5], decreases=negative[:5])
        with (root/f'salary_associations_{size}.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(associations[0]))
            writer.writeheader()
            for row in sorted(associations, key=lambda r: (-r['percent'], r['skills'])):
                writer.writerow({k: ' + '.join(v) if isinstance(v, list) else v for k,v in row.items()})
    summary['regional'] = []
    for region in json.loads(Path('config.json').read_text())['regions']:
        total = next(row for row in ref['regions'] if row[0] == region)
        combinations = {}
        for size in (1, 2, 3):
            ranked = sorted([row for row in ref['records'] if row[0] == region and len(row[1]) == size],
                            key=lambda r: (-r[2], r[1]))
            combinations[str(size)] = ranked[:3]
        summary['regional'].append(dict(region=region, vacancies=total[1], salary_n=total[2],
                                        median=total[3], top=combinations))
    trials = json.loads((root/'benchmark.json').read_text())
    assert len(trials) == 15 and all(row['exit_code'] == 0 for row in trials)
    benchmarks = []
    for case in dict.fromkeys(row['case'] for row in trials):
        case_trials = sorted([row for row in trials if row['case'] == case], key=lambda r:r['attempt'])
        assert len(case_trials) == 3
        benchmarks.append(dict(case=case, trials=case_trials,
            median_seconds=statistics.median(row['wall_seconds'] for row in case_trials),
            median_peak_mib=statistics.median(row['peak_rss_bytes']/1024**2 for row in case_trials)))
    summary['benchmarks'] = benchmarks
    (root/'analysis.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(json.dumps(dict(verified_files=len(checks), matched=summary['matched'],
        empty_skills=summary['empty_skills'], salary_count=summary['salary_count'],
        groups=len(ref['records']), digest=expected, combinations=summary['combinations'],
        benchmarks=[{k:v for k,v in b.items() if k!='trials'} for b in benchmarks]), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

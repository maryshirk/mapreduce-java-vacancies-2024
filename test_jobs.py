import csv
import io
from pathlib import Path
import unittest

from jobs import FIELDS, mapper, parse_skills, run_duckdb, run_mapreduce, run_pandas, s3_client, salary_twice, selected, load_config


CONFIG = {
    'variant': 'TEST FIXTURE ONLY',
    'title_regex': r'\bPython\b', 'roles': [],
    'sql_where': "regexp_matches(coalesce(name, ''), '\\bPython\\b', 'i')",
    'bucket': 'hh-lake', 'key': '_tests/mapreduce_fixture.csv',
    'regions': ['Москва', 'Санкт-Петербург', 'Екатеринбург'],
}


def row(identifier, title, skills, lo='', hi='', currency='RUR', gross='false', area='Москва'):
    return dict(zip(FIELDS, [identifier, title, 'Программист, разработчик', area,
                            lo, hi, currency, gross, skills]))


ROWS = [
    row('1', 'Python developer', 'Python; SQL; SQL; Linux', '100', '200'),
    row('2', 'Python\nengineer', ' SQL;Python ', '200', ''),
    row('3', 'Python engineer', '', '300', '500', area='Санкт-Петербург'),
    row('4', 'Java developer', 'Python; SQL; Linux', '900', '1100'),
    row('5', 'Python developer', 'PYTHON;SQL;Linux;Git', '400', '600', gross='true'),
    row('6', 'python developer', 'Python;SQL', '1000', '', currency='USD'),
    row('7', 'Python developer', '\tPython\t;\tSQL\n', '0', '250'),
]


class JobsTest(unittest.TestCase):
    def test_java_filter_excludes_javascript_only_and_other_roles(self):
        config = load_config(Path(__file__).parent/'config.json')
        self.assertFalse(selected(row('a', 'JavaScript developer', 'JavaScript;React'), config))
        self.assertTrue(selected(row('b', 'Backend developer', 'Java;Spring'), config))
        tester = row('c', 'Java tester', 'Java')
        tester['role'] = 'Тестировщик'
        self.assertFalse(selected(tester, config))

    def test_skill_set_and_missing_salary(self):
        self.assertEqual(parse_skills(' SQL;Python;SQL; '), ('python', 'sql'))
        self.assertEqual(salary_twice(ROWS[0]), 300)
        self.assertEqual(salary_twice(ROWS[-1]), 500)
        self.assertIsNone(salary_twice(ROWS[4]))
        self.assertIsNone(salary_twice(ROWS[5]))

    def test_mapper_invariants(self):
        result = mapper(0, ROWS, CONFIG)
        self.assertEqual(result['ids'], ['1', '2', '3', '5', '6', '7'])
        self.assertEqual(result['empty_skills'], 1)
        self.assertEqual(result['aggregates'][('__ALL__', ('python',))][0], 5)
        self.assertEqual(result['aggregates'][('__ALL__', ('linux', 'python', 'sql'))][0], 2)

    def test_independent_s3_algorithms_and_mapper_retry(self):
        stream = io.StringIO(newline='')
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(ROWS)
        s3_client().put_object(Bucket=CONFIG['bucket'], Key=CONFIG['key'],
                               Body=stream.getvalue().encode('utf-8'), ContentType='text/csv')
        try:
            reference = run_mapreduce(CONFIG, chunk_size=2, workers=1)
            parallel = run_mapreduce(CONFIG, chunk_size=2, workers=2, fault_chunk=1)
            sql = run_duckdb(CONFIG, Path(__file__).parent/'.duckdb_extensions', threads=2)
            pandas = run_pandas(CONFIG)
            for field in ('records', 'ids', 'regions', 'empty_skills'):
                self.assertEqual(reference[field], sql[field], field)
                self.assertEqual(reference[field], parallel[field], field)
                self.assertEqual(reference[field], pandas[field], field)
            self.assertEqual(reference['input_rows'], 7)
            self.assertEqual(parallel['mapper_failures'], 1)
            self.assertEqual(parallel['committed_chunks'], 4)
        finally:
            s3_client().delete_object(Bucket=CONFIG['bucket'], Key=CONFIG['key'])


if __name__ == '__main__':
    unittest.main(verbosity=2)

import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("extractor", Path(__file__).with_name("extract-clickhouse-tests.py"))
extractor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(extractor)


class ExtractionTests(unittest.TestCase):
    def test_settings_inside_functions_and_strings_are_preserved(self):
        sql = "SELECT * FROM generateRandom('x JSON', 7, SETTINGS max_array_length = 3) SETTINGS max_threads = 1"
        self.assertEqual(extractor.normalize_statement(sql), sql.rsplit(" SETTINGS", 1)[0])
        sql = "SELECT 'a  b FORMAT JSON SETTINGS x=1', $$SELECT 1; SETTINGS x=2$$ FORMAT JSON"
        self.assertEqual(extractor.normalize_statement(sql), sql.rsplit(" FORMAT", 1)[0])

    def test_quoted_semicolons_and_comments(self):
        sql = "SELECT 'a\\';--b'; SELECT $q$a; -- b$q$; SELECT `a;b`;"
        statements = extractor.split_statements(sql)
        self.assertEqual(len(statements), 3)
        self.assertEqual(extractor.normalize_statement(statements[0]), "SELECT 'a\\';--b'")
        self.assertEqual(extractor.normalize_statement("SELECT 1 -- ;\n + 2"), "SELECT 1  + 2")

    def test_negative_statement_keeps_positive_controls(self):
        sql = "SELECT 'é;'; CREATE VIEW v COMMENT AS SELECT 1; -- { clientError SYNTAX_ERROR }\nSELECT 2;"
        statements = [extractor.normalize_statement(s) for s in extractor.split_statements(sql)]
        self.assertEqual(statements, ["SELECT 'é;'", "SELECT 2"])

    def test_alternate_languages(self):
        self.assertTrue(extractor.uses_alternate_query_language("SET dialect = 'kusto';"))
        self.assertFalse(extractor.uses_alternate_query_language("SELECT 'dialect = trino'"))

    def test_invalid_argument_counts_are_not_positive_parse_tests(self):
        sql = "SELECT nth_value(x) OVER (); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }\nSELECT nth_value(x, 2) OVER ();"
        statements = [extractor.normalize_statement(s) for s in extractor.split_statements(sql)]
        self.assertEqual(statements, ["SELECT nth_value(x, 2) OVER ()"])


if __name__ == "__main__":
    unittest.main()

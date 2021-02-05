from test.integration.base import DBTIntegrationTest, use_profile
import os
import re


class TestEphemeralMulti(DBTIntegrationTest):
    @property
    def schema(self):
        return "ephemeral_020"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test__postgres(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        self.assertTablesEqual("seed", "dependent")
        self.assertTablesEqual("seed", "double_dependent")
        self.assertTablesEqual("seed", "super_dependent")
        self.assertTrue(os.path.exists(
            './target/run/test/models/double_dependent.sql'))
        with open('./target/run/test/models/double_dependent.sql', 'r') as fp:
            sql_file = fp.read()

        sql_file = re.sub(r'\d+', '', sql_file)
        expected_sql = ('create view test_ephemeral_.double_dependent__dbt_tmp as '
                        'with o__dbt__cte__base as ('
                        'select * from test_ephemeral_.seed'
                        '),  o__dbt__cte__base_copy as ('
                        'select * from o__dbt__cte__base'
                        ')-- base_copy just pulls from base. Make sure the listed'
                        '-- graph of CTEs all share the same dbt_cte__base cte'
                        "select * from o__dbt__cte__base where gender = 'Male'"
                        'union all'
                        "select * from o__dbt__cte__base_copy where gender = 'Female'"
                        )
        sql_file = "".join(sql_file.split()).lower()
        expected_sql = "".join(expected_sql.split()).lower()

        self.assertEqual(sql_file, expected_sql)

    @use_profile('snowflake')
    def test__snowflake(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        self.assertManyTablesEqual(
            ["SEED", "DEPENDENT", "DOUBLE_DEPENDENT", "SUPER_DEPENDENT"]
        )


class TestEphemeralNested(DBTIntegrationTest):
    @property
    def schema(self):
        return "ephemeral_020"

    @property
    def models(self):
        return "models-n"

    @use_profile('postgres')
    def test__postgres(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 2)

        self.assertTrue(os.path.exists(
            './target/run/test/models-n/root_view.sql'))

        with open('./target/run/test/models-n/root_view.sql', 'r') as fp:
            sql_file = fp.read()

        sql_file = re.sub(r'\d+', '', sql_file)
        expected_sql = (
            'create view test_ephemeral_.root_view__dbt_tmp as'
            'with o__dbt__cte__ephemeral_level_two as ('
            'select * from test_ephemeral_.source_table'
            '),  o__dbt__cte__ephemeral as ('
            'select * from o__dbt__cte__ephemeral_level_two'
            ')select * from o__dbt__cte__ephemeral'
        )

        sql_file = "".join(sql_file.split()).lower()
        expected_sql = "".join(expected_sql.split()).lower()
        self.assertEqual(sql_file, expected_sql)


class TestEphemeralErrorHandling(DBTIntegrationTest):
    @property
    def schema(self):
        return "ephemeral_020"

    @property
    def models(self):
        return "ephemeral-errors"

    @use_profile('postgres')
    def test__postgres_upstream_error(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'skipped')
        self.assertIn('Compilation Error', results[0].message)

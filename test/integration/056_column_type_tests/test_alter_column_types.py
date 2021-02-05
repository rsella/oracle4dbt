from test.integration.base import DBTIntegrationTest, use_profile
import yaml


class TestAlterColumnTypes(DBTIntegrationTest):
    @property
    def schema(self):
        return '056_alter_column_types'

    def run_and_alter_and_test(self, alter_column_type_args):
        self.assertEqual(len(self.run_dbt(['run'])), 1)
        self.run_dbt(['run-operation', 'test_alter_column_type', '--args', alter_column_type_args])
        self.assertEqual(len(self.run_dbt(['test'])), 1)


class TestBigQueryAlterColumnTypes(TestAlterColumnTypes):
    @property
    def models(self):
        return 'bq_models_alter_type'

    @use_profile('bigquery')
    def test_bigquery_column_types(self):
        alter_column_type_args = yaml.safe_dump({
            'model_name': 'model',
            'column_name': 'int64_col',
            'new_column_type': 'string'
        })
        self.run_and_alter_and_test(alter_column_type_args)

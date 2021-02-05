from test.integration.base import DBTIntegrationTest, use_profile


class TestSimpleCycle(DBTIntegrationTest):

    @property
    def schema(self):
        return "cycles_simple_025"

    @property
    def models(self):
        return "simple_cycle_models"

    @use_profile('postgres')
    def test_postgres_simple_cycle(self):
        message = "Found a cycle.*"
        with self.assertRaisesRegexp(Exception, message):
            self.run_dbt(["run"])


class TestComplexCycle(DBTIntegrationTest):

    @property
    def schema(self):
        return "cycles_complex_025"

    @property
    def models(self):
        return "complex_cycle_models"

    @use_profile('postgres')
    def test_postgres_simple_cycle(self):
        message = "Found a cycle.*"
        with self.assertRaisesRegexp(Exception, message):
            self.run_dbt(["run"])

from test.integration.base import DBTIntegrationTest, use_profile
from pytest import mark


# postgres sometimes fails with an internal error if you run these tests too close together.
def postgres_error(err, *args):
    msg = str(err)
    if 'tuple concurrently updated' in msg:
        return True
    return False


@mark.flaky(rerun_filter=postgres_error)
class TestPermissions(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "permission_tests_010"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_no_create_schema_permissions(self):
        # the noaccess user does not have permissions to create a schema -- this should fail

        self.create_noaccess_schema(with_all_privileges=False)
        try:
            self.run_sql('drop user "{}" cascade'.format(self.unique_schema()))
        except:
            pass
        # with self.assertRaises(RuntimeError):
        self.run_dbt(['run', '--target', 'noaccess'], expect_pass=False)

    @use_profile('postgres')
    def test_postgres_create_schema_permissions(self):
        # now it should work!
        self.create_noaccess_schema(with_all_privileges=True)
        # self.run_sql('grant create on database {} to noaccess'.format(self.default_database))
        # self.run_sql('grant usage, create on schema "{}" to noaccess'.format(self.unique_schema()))
        # self.run_sql('grant select on all tables in schema "{}" to noaccess'.format(self.unique_schema()))

        results = self.run_dbt(['run', '--target', 'noaccess'])
        self.assertEqual(len(results), 1)

    def drop_noaccess_schema(self):
        try:
            self.run_sql('drop user noaccess cascade')
        except:
            pass

    def create_noaccess_schema(self, with_all_privileges):
        self.drop_noaccess_schema()
        self.run_sql('create user noaccess identified by noaccess')
        if with_all_privileges:
            self.run_sql('grant all privileges to noaccess')
        else:
            self.run_sql('grant connect to noaccess')
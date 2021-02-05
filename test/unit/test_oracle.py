import decimal
import random
import unittest
from unittest.mock import MagicMock, call

import cx_Oracle
import dbt.flags as flags
from cx_Oracle import DatabaseError
from dbt.adapters.base.query_headers import MacroQueryStringSetter
from dbt.clients import agate_helper
from dbt.exceptions import ValidationException, DbtConfigError
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.parser import ParseResult
from dbt.task.debug import DebugTask

from dbt.adapters.oracle import OracleAdapter, Plugin as OraclePlugin
from .utils import *


class TestOracleAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True
        project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
            'config-version': 2,
        }
        profile_cfg = {
            'outputs': {
                'test': {
                    'type': 'oracle',
                    'service': 'test',
                    'username': 'root',
                    'host': 'thishostshouldnotexist',
                    'password': 'password',
                    'port': 1521,
                    'schema': 'public'
                }
            },
            'target': 'test'
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = OracleAdapter(self.config)
            inject_adapter(self._adapter, OraclePlugin)
        return self._adapter

    @mock.patch('dbt.adapters.oracle.connections.oracle')
    def test_acquire_connection_validations(self, oracle):
        try:
            connection = self.adapter.acquire_connection('dummy')
        except ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))
        except BaseException as e:
            self.fail('acquiring connection failed with unknown exception: {}'
                      .format(str(e)))
        self.assertEqual(connection.type, 'oracle')

        oracle.connect.assert_not_called()
        connection.handle
        oracle.connect.assert_called_once()

    @mock.patch('dbt.adapters.oracle.connections.oracle')
    def test_acquire_connection(self, oracle):
        connection = self.adapter.acquire_connection('dummy')

        oracle.connect.assert_not_called()
        connection.handle
        self.assertEqual(connection.state, 'open')
        self.assertNotEqual(connection.handle, None)
        oracle.connect.assert_called_once()

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections[key] = mock_connection('master')
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_single(self):
        master = mock_connection('master')
        model = mock_connection('model')
        key = self.adapter.connections.get_thread_identifier()
        model.handle.get_backend_pid.return_value = 42
        self.adapter.connections.thread_connections.update({
            key: master,
            1: model,
        })
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 1)
        model.handle.cancel.assert_called_once()
        model.handle.add_query.assert_not_called()

    @mock.patch('dbt.adapters.oracle.connections.oracle')
    def test_changed_as_sysdba(self, oracle):
        self.config.credentials = self.config.credentials.replace(as_sysdba=True)
        connection = self.adapter.acquire_connection('dummy')

        oracle.connect.assert_not_called()
        connection.handle
        oracle.connect.assert_called_once_with(
            'root',
            'password',
            'thishostshouldnotexist:1521/test',
            mode=oracle.SYSDBA
        )

    @mock.patch('dbt.adapters.oracle.connections.oracle')
    def test_default_auth(self, oracle):
        connection = self.adapter.acquire_connection('dummy')

        oracle.connect.assert_not_called()
        connection.handle
        oracle.connect.assert_called_once_with(
            'root',
            'password',
            'thishostshouldnotexist:1521/test',
            mode=oracle.DEFAULT_AUTH
        )

    @mock.patch.object(OracleAdapter, 'execute_macro')
    @mock.patch.object(OracleAdapter, '_get_catalog_schemas')
    def test_get_catalog_various_schemas(self, mock_get_schemas, mock_execute):
        column_names = ['table_database', 'table_schema', 'table_name']
        rows = [
            ('dbt', 'foo', 'bar'),
            ('dbt', 'FOO', 'baz'),
            ('dbt', None, 'bar'),
            ('dbt', 'quux', 'bar'),
            ('dbt', 'skip', 'bar'),
        ]
        mock_execute.return_value = agate.Table(rows=rows,
                                                column_names=column_names)

        mock_get_schemas.return_value.items.return_value = [(mock.MagicMock(database='dbt'), {'foo', 'FOO', 'quux'})]

        mock_manifest = mock.MagicMock()
        mock_manifest.get_used_schemas.return_value = {('dbt', 'foo'),
                                                       ('dbt', 'quux')}

        catalog, exceptions = self.adapter.get_catalog(mock_manifest)
        self.assertEqual(
            set(map(tuple, catalog)),
            {('dbt', 'foo', 'bar'), ('dbt', 'FOO', 'baz'), ('dbt', 'quux', 'bar')}
        )
        self.assertEqual(exceptions, [])

    @mock.patch('dbt.adapters.oracle.connections.oracle')
    def test_split_on_semicolon(self, oracle: MagicMock):
        self.adapter.acquire_connection('test_semicolon')

        query1 = "SELECT * FROM DUAL"
        query2 = "SELECT 'TEST' FROM DUAL"
        multi_query = f'{query1};\n{query2}'

        oracle.connect.assert_not_called()
        self.adapter.execute(multi_query)
        oracle.assert_has_calls(
            [
                call.connect().cursor().execute(query1),
                call.connect().cursor().execute(query2)
            ],
            any_order=True
        )

    @mock.patch('dbt.adapters.oracle.connections.oracle')
    def test_dont_split_semicolon_comment(self, oracle: MagicMock):
        self.adapter.acquire_connection('test_semicolon')

        query1 = "SELECT * FROM DUAL"
        query2 = "SELECT 'TEST' FROM DUAL"
        multi_query = f'--{query1};\n{query2}'

        oracle.connect.assert_not_called()
        self.adapter.execute(multi_query)
        oracle.assert_has_calls(
            [
                call.connect().cursor().execute(multi_query)
            ],
            any_order=True
        )

    @mock.patch('dbt.adapters.oracle.connections.oracle')
    def test_complex_semicolon_split(self, oracle: MagicMock):
        self.adapter.acquire_connection('test_semicolon')

        query1 = "--SELECT * FROM DUAL;SELECT 'A' FROM DUAL WHERE 1 = 0;;;"
        query2 = "SELECT 'TEST' FROM DUAL"
        query3 = "SELECT 'A' FROM DUAL"
        query4 = "UNION ALL SELECT 'B' FROM DUAL"

        first_query = f'{query1};\n{query2}'
        second_query = f'{query3} {query4}'
        multi_query = f'{first_query};\n{second_query}'

        oracle.connect.assert_not_called()
        self.adapter.execute(multi_query)
        oracle.assert_has_calls(
            [
                call.connect().cursor().execute(first_query),
                call.connect().cursor().execute(second_query)
            ],
            any_order=True
        )


class TestConnectingOracleAdapter(unittest.TestCase):
    def setUp(self):
        self.target_dict = {
            'type': 'oracle',
            'service': 'test',
            'username': 'root',
            'host': 'thishostshouldnotexist',
            'password': 'password',
            'port': 1521,
            'schema': 'public'
        }

        profile_cfg = {
            'outputs': {
                'test': self.target_dict,
            },
            'target': 'test'
        }
        project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
            'quoting': {
                'identifier': False,
                'schema': True,
            },
            'config-version': 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)

        self.handle = mock.MagicMock(spec=cx_Oracle.Connection)
        self.cursor = self.handle.cursor.return_value
        self.mock_execute = self.cursor.execute
        self.patcher = mock.patch('dbt.adapters.oracle.connections.oracle')
        self.oracle = self.patcher.start()

        self.load_patch = mock.patch('dbt.parser.manifest.make_parse_result')
        self.mock_parse_result = self.load_patch.start()
        self.mock_parse_result.return_value = ParseResult.rpc()

        self.oracle.connect.return_value = self.handle
        self.adapter = OracleAdapter(self.config)
        self.adapter._macro_manifest_lazy = load_internal_manifest_macros(self.config)
        self.adapter.connections.query_header = MacroQueryStringSetter(self.config, self.adapter._macro_manifest_lazy)

        self.qh_patch = mock.patch.object(self.adapter.connections.query_header, 'add')
        self.mock_query_header_add = self.qh_patch.start()
        self.mock_query_header_add.side_effect = lambda q: '/* dbt */\n{}'.format(q)
        self.adapter.acquire_connection()
        inject_adapter(self.adapter, OraclePlugin)

    def tearDown(self):
        # we want a unique self.handle every time.
        self.adapter.cleanup_connections()
        self.qh_patch.stop()
        self.patcher.stop()
        self.load_patch.stop()
        clear_plugin(OraclePlugin)

    # def test_quoting_on_drop_schema(self):
    #     relation = self.adapter.Relation.create(
    #         database='oracle', schema='test_schema',
    #         quote_policy=self.adapter.config.quoting,
    #     )
    #     self.adapter.drop_schema(relation)
    #
    #     self.mock_execute.assert_has_calls([
    #         mock.call('/* dbt */\ndrop user test_schema cascade', None)
    #     ])

    # def test_quoting_on_drop(self):
    #     relation = self.adapter.Relation.create(
    #         database='oracle',
    #         schema='test_schema',
    #         identifier='test_table',
    #         type='table',
    #         quote_policy=self.adapter.config.quoting,
    #     )
    #     self.adapter.drop_relation(relation)
    #     self.mock_execute.assert_any_call([
    #         mock.call('/* dbt */\ndrop table test_table', None)
    #     ])

    def test_quoting_on_truncate(self):
        relation = self.adapter.Relation.create(
            database='oracle',
            schema='test_schema',
            identifier='test_table',
            type='table',
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.truncate_relation(relation)
        self.mock_execute.assert_called_once_with('/* dbt */\ntruncate table test_schema.test_table')

    # def test_quoting_on_rename(self):
    #     random.seed(1)  # set the seed to obtain the same random number every time
    #     from_relation = self.adapter.Relation.create(
    #         database='oracle',
    #         schema='test_schema',
    #         identifier='table_a',
    #         type='table',
    #         quote_policy=self.adapter.config.quoting,
    #     )
    #     to_relation = self.adapter.Relation.create(
    #         database='oracle',
    #         schema='test_schema',
    #         identifier='table_b',
    #         type='table',
    #         quote_policy=self.adapter.config.quoting,
    #     )
    #
    #     self.adapter.rename_relation(
    #         from_relation=from_relation,
    #         to_relation=to_relation
    #     )
    #     self.mock_execute.assert_called_with(
    #         '/* dbt */\nBEGIN\n            test_schema.DBT_RENAME__17611(\'table_a\', \'table_b\');\n        END;'
    #     )

    def test_debug_connection_ok(self):
        DebugTask.validate_connection(self.target_dict)
        self.mock_execute.assert_has_calls([
            mock.call('/* dbt */\nselect 1 as id from dual')
        ])

    def test_debug_connection_fail_nopass(self):
        del self.target_dict['password']
        with self.assertRaises(DbtConfigError):
            DebugTask.validate_connection(self.target_dict)

    def test_connection_fail_select(self):
        self.mock_execute.side_effect = DatabaseError()
        with self.assertRaises(DbtConfigError):
            DebugTask.validate_connection(self.target_dict)
        self.mock_execute.assert_has_calls([
            mock.call('/* dbt */\nselect 1 as id from dual')
        ])


class TestOracleFilterCatalog(unittest.TestCase):
    def test__catalog_filter_table(self):
        manifest = mock.MagicMock()
        manifest.get_used_schemas.return_value = [['a', 'B'], ['a', '1234']]
        column_names = ['table_name', 'table_database', 'table_schema', 'something']
        rows = [
            ['foo', 'a', 'b', '1234'],  # include
            ['foo', 'a', '1234', '1234'],  # include, w/ table schema as str
            ['foo', 'c', 'B', '1234'],  # skip
            ['1234', 'A', 'B', '1234'],  # include, w/ table name as str
        ]
        table = agate.Table(
            rows, column_names, agate_helper.DEFAULT_TYPE_TESTER
        )

        result = OracleAdapter._catalog_filter_table(table, manifest)
        assert len(result) == 3
        for row in result.rows:
            assert isinstance(row['table_schema'], str)
            assert isinstance(row['table_database'], str)
            assert isinstance(row['table_name'], str)
            assert isinstance(row['something'], decimal.Decimal)


class TestPostgresAdapterConversions(TestAdapterConversions):
    def test_convert_text_type(self):
        dbt_additional_space = OracleAdapter.oracle_additional_text_space_for_dbt()
        rows = [
            ['', 'a1', 'stringval1'],
            ['', 'a2', 'stringvalasdfasdfasdfa'],
            ['', 'a3', 'stringval3'],
        ]
        agate_table = self._make_table_of(rows, agate.Text)
        oracle_text_type = 'VARCHAR2({} CHAR)'
        expected = [
            oracle_text_type.format(0 + dbt_additional_space),
            oracle_text_type.format(2 + dbt_additional_space),
            oracle_text_type.format(22 + dbt_additional_space)
        ]
        for col_idx, expect in enumerate(expected):
            assert OracleAdapter.convert_text_type(agate_table, col_idx) == expect

    def test_convert_number_type(self):
        rows = [
            ['', '23.98', '-1'],
            ['', '12.78', '-2'],
            ['', '79.41', '-3'],
        ]
        agate_table = self._make_table_of(rows, agate.Number)
        expected = ['NUMBER', 'NUMBER', 'NUMBER']
        for col_idx, expect in enumerate(expected):
            assert OracleAdapter.convert_number_type(agate_table, col_idx) == expect

    def test_convert_boolean_type(self):
        rows = [
            ['', 'false', 'true'],
            ['', 'false', 'false'],
            ['', 'false', 'true'],
        ]
        agate_table = self._make_table_of(rows, agate.Boolean)
        expected = ['NUMBER', 'NUMBER', 'NUMBER']
        for col_idx, expect in enumerate(expected):
            assert OracleAdapter.convert_boolean_type(agate_table, col_idx) == expect

    def test_convert_datetime_type(self):
        rows = [
            ['', '20190101T01:01:01Z', '2019-01-01 01:01:01'],
            ['', '20190102T01:01:01Z', '2019-01-01 01:01:01'],
            ['', '20190103T01:01:01Z', '2019-01-01 01:01:01'],
        ]
        agate_table = self._make_table_of(rows, [agate.DateTime, agate_helper.ISODateTime, agate.DateTime])
        expected = ['DATE', 'DATE', 'DATE']
        for col_idx, expect in enumerate(expected):
            assert OracleAdapter.convert_datetime_type(agate_table, col_idx) == expect

    def test_convert_date_type(self):
        rows = [
            ['', '2019-01-01', '2019-01-04'],
            ['', '2019-01-02', '2019-01-04'],
            ['', '2019-01-03', '2019-01-04'],
        ]
        agate_table = self._make_table_of(rows, agate.Date)
        expected = ['TIMESTAMP', 'TIMESTAMP', 'TIMESTAMP']
        for col_idx, expect in enumerate(expected):
            assert OracleAdapter.convert_date_type(agate_table, col_idx) == expect

    def test_convert_time_type(self):
        # dbt's default type testers actually don't have a TimeDelta at all.
        agate.TimeDelta
        rows = [
            ['', '120s', '10s'],
            ['', '3m', '11s'],
            ['', '1h', '12s'],
        ]
        agate_table = self._make_table_of(rows, agate.TimeDelta)
        expected = ['TIMESTAMP', 'TIMESTAMP', 'TIMESTAMP']
        for col_idx, expect in enumerate(expected):
            assert OracleAdapter.convert_time_type(agate_table, col_idx) == expect

import re
from typing import Optional, List, Tuple, Union

import agate
import sqlparse
from dbt.adapters.base import BaseRelation
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.connection import AdapterResponse

from .connections import OracleConnectionManager
from .relation import OracleRelation, OracleColumn


class OracleAdapter(SQLAdapter):
    ConnectionManager = OracleConnectionManager
    Relation = OracleRelation
    Column = OracleColumn

    def date_function(self) -> str:
        return "SYSDATE"

    def debug_query(self) -> None:
        self.execute('select 1 as id from dual')

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = 'hour') -> str:
        # in oracle is "interval '1' hour" and not "interval '1 hour'"
        return f"{add_to} + interval '{number}' {interval}"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        max_str_length = agate_table.aggregate(agate.MaxLength(col_idx)) + cls.oracle_additional_text_space_for_dbt()
        return f"VARCHAR2({max_str_length} CHAR)"

    @classmethod
    def oracle_additional_text_space_for_dbt(cls) -> int:
        return 20

    @classmethod
    def convert_number_type(
            cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "NUMBER"

    @classmethod
    def convert_boolean_type(
            cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "NUMBER"

    @classmethod
    def convert_datetime_type(
            cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "DATE"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "TIMESTAMP"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "TIMESTAMP"

    def quote(self, identifier):
        return identifier  # never quote on Oracle adapter

    # Oracle doesn't support 'AS' for table alias
    # Oracle EXCEPT operator is MINUS
    def get_rows_different_sql(self, relation_a: BaseRelation, relation_b: BaseRelation,
                               column_names: Optional[List[str]] = None,
                               except_operator: str = 'MINUS') -> str:
        """Generate SQL for a query that returns a single row with a two
                columns: the number of rows that are different between the two
                relations and the number of mismatched rows.
                """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ', '.join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )

        return sql

    def execute(self, sql: str, auto_begin: bool = False, fetch: bool = False) -> Tuple[
        Union[str, AdapterResponse], agate.Table]:

        last_call = None
        for query in OracleAdapter._oracle_query_parse(sql):
            last_call = super().execute(query, auto_begin, fetch)

        # force commit at the end, avoid bugs that are hard as fuck to catch (like insert in hooks)
        # may be a problem?
        self.connections.add_commit_query()

        return last_call

    @staticmethod
    def _oracle_query_parse(sql):
        sql_list = []

        # plsql check
        # for plsql cx_Oracle needs the ending ;
        if 'end;' in sql.lower():
            return [sql]

        # split multiple statements, cx_Oracle can execute one statement at time

        for sql_query in sqlparse.split(sql):

            if sql_query.endswith(';'):
                sql_query = sql_query[:-1]

            if sql_query is not None and sql_query.strip() != '':
                sql_list.append(sql_query)

        return sql_list


# Oracle doesn't support 'AS' for table alias
COLUMNS_EQUAL_SQL = '''
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing 
    FROM (
            (SELECT {columns} FROM {relation_a} {except_op}
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} {except_op}
             SELECT {columns} FROM {relation_a})
        ) a
), oracle_table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), oracle_table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        oracle_table_a.num_rows - oracle_table_b.num_rows as difference
    from oracle_table_a, oracle_table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
join diff_count using (id)
'''.strip()

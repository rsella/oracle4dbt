import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import ContextManager, Any, Optional, Tuple, Union

import cx_Oracle as oracle
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection, AdapterResponse
from dbt.exceptions import RuntimeException, FailedToConnectException
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class OracleCredentials(Credentials):
    username: str
    password: str
    host: str
    port: int
    as_sysdba: Optional[bool] = False
    nls_date_format: Optional[str] = None

    _ALIASES = {
        'service': 'database'
    }

    @property
    def type(self):
        return 'oracle'

    def _connection_keys(self):
        return 'host', 'port', 'database', 'schema', 'username', 'as_sysdba', 'nls_date_format'

    @property
    def connection_string(self):
        return f"{self.username}@{self.host}:{self.port}/{self.database}{' AS SYSDBA' if self.as_sysdba else ''}"


class OracleConnectionManager(SQLConnectionManager):
    TYPE = 'oracle'

    @classmethod
    def get_response(cls, cursor: Any) -> Union[AdapterResponse, str]:
        return AdapterResponse(_message='OK', code=cursor.statement, rows_affected=cursor.rowcount)

    def cancel(self, connection: Connection):
        if connection.handle is not None:
            connection.handle.cancel()

    @classmethod
    def get_status(cls, cursor: Any) -> str:
        return f'OK - {cursor.rowcount} rows affected'

    @contextmanager
    def exception_handler(self, sql: str) -> ContextManager:
        try:
            yield
        except Exception as e:
            logger.error("Error running SQL: {}".format(sql))
            logger.error("Rolling back transaction.")
            self.rollback_if_open()

            if isinstance(e, RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise RuntimeException(e) from e

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        try:
            connection_string = '{}:{}/{}'
            handle = oracle.connect(
                credentials.username,
                credentials.password,
                connection_string.format(credentials.host, credentials.port, credentials.database),
                mode=oracle.SYSDBA if credentials.as_sysdba else oracle.DEFAULT_AUTH
            )
            connection.state = 'open'
            connection.handle = handle

            if credentials.nls_date_format is not None:
                handle.cursor().execute(f"ALTER SESSION SET NLS_DATE_FORMAT = '{credentials.nls_date_format}'")
                handle.cursor().execute(f"ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '{credentials.nls_date_format}XFF'")
                handle.cursor().execute(f"ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = '{credentials.nls_date_format}XFF TZR'")


        except Exception as e:
            logger.debug(
                f"Got an error when attempting to open a oracle connection. Connection string = {credentials.connection_string}. Error: {e}"
            )

            connection.handle = None
            connection.state = 'fail'

            raise FailedToConnectException(f"{str(e)} using connection string {credentials.connection_string}")

        return connection

    def add_query(self, sql: str, auto_begin: bool = True, bindings: Optional[Any] = None,
                  abridge_sql_log: bool = False) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug('Using {} connection "{}".'
                     .format(self.TYPE, connection.name))

        with self.exception_handler(sql):

            if abridge_sql_log:
                log_sql = '{}...'.format(sql[:512])
            else:
                log_sql = sql

            logger.debug(
                'On {connection_name}: {sql}',
                connection_name=connection.name,
                sql=log_sql,
            )
            pre = time.time()

            cursor = connection.handle.cursor()

            execute_many = False
            if 'insert' in sql.lower() and bindings is not None:
                execute_many = True

            # cx_Oracle doesn't like if bindings is None and it is passed to execute. good job oracle
            if bindings is not None:
                if execute_many:
                    cursor.executemany(sql, bindings)
                else:
                    cursor.execute(sql, bindings)
            else:
                cursor.execute(sql)

            logger.debug(
                "SQL status: {status} in {elapsed:0.2f} seconds",
                status=self.get_status(cursor),
                elapsed=(time.time() - pre)
            )
            return connection, cursor

    def add_begin_query(self):
        self.get_thread_connection().handle.begin()

    def add_commit_query(self):
        self.get_thread_connection().handle.commit()

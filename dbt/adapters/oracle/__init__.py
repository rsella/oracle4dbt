from dbt.adapters.base import AdapterPlugin

from dbt.adapters.oracle.connections import OracleConnectionManager
from dbt.adapters.oracle.connections import OracleCredentials
from dbt.adapters.oracle.impl import OracleAdapter
from dbt.include import oracle

Plugin = AdapterPlugin(
    adapter=OracleAdapter,
    credentials=OracleCredentials,
    include_path=oracle.PACKAGE_PATH)

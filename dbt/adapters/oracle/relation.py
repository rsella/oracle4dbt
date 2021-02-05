from dataclasses import dataclass
from typing import Optional, Iterator, Tuple, ClassVar, Dict, Any

from dbt.adapters.base import BaseRelation, Column
from dbt.contracts.relation import ComponentName, Policy
from dbt.exceptions import RuntimeException


@dataclass(frozen=True, eq=False, repr=False)
class OracleRelation(BaseRelation):
    def __post_init__(self):
        if (self.identifier is not None and self.type is not None and
                len(self.identifier) > self.relation_max_name_length()):
            raise RuntimeException(
                f"Relation name '{self.identifier}' "
                f"is longer than {self.relation_max_name_length()} characters"
            )

    @classmethod
    def get_default_include_policy(cls) -> Policy:
        return Policy(database=False)  # oracle use schema.object_name, not database.schema.object_name

    @classmethod
    def get_default_quote_policy(cls) -> Policy:
        return Policy(database=False, schema=False, identifier=False)  # usually identifiers in oracle are not quotes

    def _render_iterator(self) -> Iterator[Tuple[Optional[ComponentName], Optional[str]]]:
        self._force_oracle_policies()
        return super()._render_iterator()

    def _is_exactish_match(self, field: ComponentName, value: str) -> bool:
        self._force_oracle_policies()
        return self.path.get_lowered_part(field) == value.lower()  # always return check on lowered parts (ignore case)

    # Oracle doesn't support table_name that starts with _
    @staticmethod
    def add_ephemeral_prefix(name: str):
        return f'o{BaseRelation.add_ephemeral_prefix(name)}'

    @staticmethod
    def relation_max_name_length():
        # oracle max is 128 from DB 12.2, 30 is some spacing for DBT suffixes
        return 128 - 30

    def _force_oracle_policies(self):
        object.__setattr__(self, 'include_policy', self.get_default_include_policy())  # force oracle include policy
        object.__setattr__(self, 'quote_policy', self.get_default_quote_policy())  # force oracle quote policy


class OracleColumn(Column):
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        # in base class
        'STRING': 'VARCHAR2',
        'TIMESTAMP': 'TIMESTAMP',
        'FLOAT': 'NUMBER',
        'INTEGER': 'NUMBER',

        # all text types conversions
        'TEXT': 'VARCHAR2',
        'CHARACTER VARYING': 'VARCHAR2',
        'CHARACTER': 'VARCHAR2',
        'VARCHAR': 'VARCHAR2',

        # all integer/float types conversions
        'REAL': 'NUMBER',
        'FLOAT4': 'NUMBER',
        'DOUBLE PRECISION': 'NUMBER',
        'FLOAT8': 'NUMBER',
        'SMALLINT': 'NUMBER',
        'BIGINT': 'NUMBER',
        'SMALLSERIAL': 'NUMBER',
        'SERIAL': 'NUMBER',
        'BIGSERIAL': 'NUMBER',
        'INT2': 'NUMBER',
        'INT4': 'NUMBER',
        'INT8': 'NUMBER',
        'SERIAL2': 'NUMBER',
        'SERIAL4': 'NUMBER',
        'SERIAL8': 'NUMBER',
        'NUMERIC': 'NUMBER',
        'DECIMAL': 'NUMBER'
    }

    @property
    def data_type(self) -> str:
        if self.is_string():
            return OracleColumn.string_type(self.string_size())
        elif self.is_numeric():
            return OracleColumn.numeric_type(self.dtype, self.numeric_precision,
                                             self.numeric_scale)
        else:
            return self.dtype

    def is_string(self) -> bool:
        return self.dtype.lower() in ['varchar2', 'char']

    def is_float(self):
        return self.dtype.lower() == 'number'

    def is_integer(self) -> bool:
        return self.dtype.lower() == 'number'

    def is_numeric(self) -> bool:
        return self.dtype.lower() == 'number'

    def literal(self, value: Any) -> str:
        return "CAST({} AS {})".format(value, self.data_type)

    @classmethod
    def string_type(cls, size: int) -> str:
        return "VARCHAR2({} CHAR)".format(size)

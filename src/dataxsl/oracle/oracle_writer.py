"""Independent Oracle sink for the one-reader, N-writer process pipeline."""
from contextlib import contextmanager
import logging
import re
from datetime import date, datetime, time
from time import perf_counter
from typing import Any

import pandas as pd
import oracledb

from dataxsl.config import validate_schema
from dataxsl.register import PluginRegistry
from dataxsl.transform import convert_value, COLUMN_TYPES_SCHEMA
from dataxsl.utils import resolve_secret
from dataxsl.writer import Writer

logger = logging.getLogger(__name__)

SQL_SCHEMA = {'anyOf': [
    {'type': 'null'}, {'type': 'string', 'pattern': r'\S'},
    {'type': 'array', 'items': {'type': 'string', 'pattern': r'\S'}},
]}
# Each name is either an ordinary Oracle identifier or explicitly double-quoted.
NAME_PATTERN = r'(?:[A-Za-z][A-Za-z0-9_$#]*|"(?:[^"\x00]|"")+")'
DB_CONFIG_SCHEMA = {
    'type': 'object', 'required': ['user', 'password'], 'additionalProperties': False,
    'oneOf': [
        {'required': ['dsn'], 'not': {'anyOf': [{'required': [key]} for key in
                                              ('host', 'port', 'service_name', 'sid')]}},
        {'required': ['host', 'service_name'], 'not': {'anyOf': [{'required': ['dsn']}, {'required': ['sid']}]}},
        {'required': ['host', 'sid'], 'not': {'anyOf': [{'required': ['dsn']}, {'required': ['service_name']}]}},
    ],
    'properties': {
        **{name: {'type': 'string', 'minLength': 1} for name in
           ('host', 'user', 'dsn', 'service_name', 'sid', 'config_dir')},
        'password': {'type': 'string'},
        'port': {'type': 'integer', 'minimum': 1, 'maximum': 65535},
        'schema': {'type': 'string', 'pattern': '^' + NAME_PATTERN + '$'},
        'autocommit': {'type': 'boolean', 'const': False},
        'tcp_connect_timeout': {'type': 'number', 'exclusiveMinimum': 0},
        'call_timeout': {'type': 'integer', 'minimum': 1},
    },
}
ORACLE_WRITER_SCHEMA = {
    'type': 'object', 'required': ['db_config', 'table', 'column'],
    'additionalProperties': False,
    'properties': {
        'table': {'type': 'string', 'pattern': '^' + NAME_PATTERN + r'(?:\.' + NAME_PATTERN + ')?$'},
        'column': {'type': 'array', 'minItems': 1, 'uniqueItems': True,
                   'items': {'type': 'string', 'pattern': '^' + NAME_PATTERN + '$'}},
        'writerMode': {'type': 'string', 'const': 'insert'},
        'pre_sql': SQL_SCHEMA, 'post_sql': SQL_SCHEMA, 'session': SQL_SCHEMA,
        'additive_attr': {'type': ['object', 'null'], 'propertyNames': {'type': 'string'},
                          'additionalProperties': {'type': ['string', 'number', 'boolean', 'null']}},
        'column_types': COLUMN_TYPES_SCHEMA,
        'db_config': DB_CONFIG_SCHEMA,
    },
}


def sql_list(value):
    if value is None:
        return []
    return [value] if isinstance(value, str) else value


def identifier(value):
    if not isinstance(value, str) or re.fullmatch(NAME_PATTERN, value) is None:
        raise ValueError('Invalid Oracle identifier')
    return value if value.startswith('"') else '"' + value.upper() + '"'


@PluginRegistry.register_writer('oracle_writer')
class OracleWriter(Writer):
    schema: dict[str, Any] = ORACLE_WRITER_SCHEMA

    def __init__(self, **config: Any):
        self._extra_config = {name: value for name, value in config.items() if name not in self.schema['properties']}
        # Preserve raw JSON values until Schema validation; never coerce bad types.
        db_config: Any = config.get('db_config')
        self.db_config: dict[str, Any] = dict(db_config) if isinstance(db_config, dict) else db_config
        self.table: str = config.get('table', '')
        self.column = config.get('column', [])
        self.pre_sql = config.get('pre_sql')
        self.post_sql = config.get('post_sql')
        self.session = config.get('session', [])
        self.additive_attr = config.get('additive_attr', {})
        self.mode = config.get('writerMode', 'insert')
        self.column_types = config.get('column_types', {})
        self.rows_written = 0
        self.sql_insert: str | None = None
        self.timings: dict[str, float] = {}

    def validate(self):
        validate_schema({'db_config': self.db_config, 'table': self.table, 'column': self.column,
                         'writerMode': self.mode, 'pre_sql': self.pre_sql, 'post_sql': self.post_sql,
                         'session': self.session, 'additive_attr': self.additive_attr,
                         'column_types': self.column_types, **self._extra_config},
                        self.schema, 'writer.parameter')
        # Cross-field references and external resources are outside static Schema rules.
        if set(self.column_types) - set(self.column):
            raise ValueError('column_types must refer to target columns')
        self.pre_sql = sql_list(self.pre_sql)
        self.post_sql = sql_list(self.post_sql)
        self.session = sql_list(self.session)
        self.additive_attr = self.additive_attr or {}
        if len({identifier(column) for column in self.column}) != len(self.column):
            raise ValueError('Oracle target columns contain duplicate identifiers')
        self.db_config['autocommit'] = False
        if 'dsn' not in self.db_config:
            self.db_config.setdefault('port', 1521)
        self.db_config.setdefault('tcp_connect_timeout', 10)
        self.db_config.setdefault('call_timeout', 30000)
        self.db_config['password'] = resolve_secret(self.db_config['password'])
        parts = re.findall(NAME_PATTERN, self.table)
        if len(parts) == 1 and 'schema' in self.db_config:
            parts.insert(0, self.db_config['schema'])
        table = '.'.join(identifier(part) for part in parts)
        columns = ', '.join(identifier(column) for column in self.column)
        placeholders = ', '.join(f':{index}' for index in range(1, len(self.column) + 1))
        self.sql_insert = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'

    def _connect(self):
        return oracledb.connect(**{key: value for key, value in self.db_config.items()
                                  if key not in {'schema', 'autocommit', 'call_timeout'}})

    def validate_columns(self, columns):
        columns = list(columns)
        for name in self.additive_attr:
            if columns.count(name) > 1:
                raise ValueError('Ambiguous additive column')
            if name not in columns:
                columns.append(name)
        if len(columns) != len(self.column):
            raise ValueError('Source/target column count mismatch; check reader output and writer.column')

    @contextmanager
    def _connection(self):
        connection = cursor = None
        failed = False
        try:
            with self._timed('connection_seconds'):
                connection = self._connect()
                connection.autocommit = False
                connection.call_timeout = self.db_config['call_timeout']
                cursor = connection.cursor()
                if 'schema' in self.db_config:
                    cursor.execute('ALTER SESSION SET CURRENT_SCHEMA = ' + identifier(self.db_config['schema']))
                for sql in self.session:
                    cursor.execute(sql)

            yield connection, cursor
        except BaseException:
            failed = True
            if connection is not None:
                try:
                    connection.rollback()
                except Exception:
                    logger.error('Rollback failed; database outcome may be uncertain')
            raise
        finally:
            close_errors = []
            for resource in (cursor, connection):
                if resource is not None:
                    try:
                        resource.close()
                    except Exception as exc:
                        close_errors.append(exc)
                        logger.error('Failed to close Oracle resource: %s', type(exc).__name__)
            if close_errors and not failed:
                raise close_errors[0]

    def _execute_sql(self, statements):
        if statements:
            with self._connection() as (connection, cursor):
                for sql in statements:
                    cursor.execute(sql)
                connection.commit()

    def pre_deal(self):
        self._execute_sql(self.pre_sql)

    def post_deal(self):
        self._execute_sql(self.post_sql)

    def _batch_values(self, rows):
        if not isinstance(rows, pd.DataFrame):
            raise TypeError('Writer expects DataFrame batches')
        self.validate_columns(rows.columns)
        frame = rows.copy()
        for name, value in self.additive_attr.items():
            frame[name] = value
        values = []
        for offset, row in enumerate(frame.itertuples(index=False, name=None)):
            converted = []
            for column, value in zip(self.column, row):
                try:
                    value = convert_value(value, self.column_types.get(column))
                    # NUMBER(1) is portable to Oracle versions without SQL BOOLEAN.
                    if isinstance(value, bool):
                        value = int(value)
                    elif isinstance(value, time):
                        value = value.isoformat()  # Oracle has no standalone TIME type.
                    elif isinstance(value, pd.Timestamp):
                        value = value.to_pydatetime()
                    converted.append(value)
                except Exception:
                    raise ValueError(f'Field conversion failed at batch row {offset + 1}, target {column}') from None
            values.append(tuple(converted))
        return values

    @contextmanager
    def _timed(self, stage):
        started = perf_counter()
        try:
            yield
        finally:
            self.timings[stage] = self.timings.get(stage, 0.0) + perf_counter() - started

    def write_parallel(self, queue):
        self.rows_written = 0
        self.timings = dict.fromkeys(('connection_seconds', 'transform_seconds',
                                     'execute_seconds', 'commit_seconds'), 0.0)
        with self._connection() as (connection, cursor):
            while True:
                rows = queue.get()

                if rows is None:
                    break
                with self._timed('transform_seconds'):
                    values = self._batch_values(rows)
                if values:
                    # logger.debug(f'values is %s...', values)
                    with self._timed('execute_seconds'):
                        # Reset bindings each batch so an all-NULL batch does not lock
                        # subsequent numeric/date batches to string bindings.
                        sizes = []
                        for index in range(len(self.column)):
                            sample = next((row[index] for row in values if row[index] is not None), None)
                            sizes.append(oracledb.DB_TYPE_TIMESTAMP if isinstance(sample, datetime)
                                         else oracledb.DB_TYPE_DATE if isinstance(sample, date) else None)
                        cursor.setinputsizes(*sizes)
                        cursor.executemany(self.sql_insert, values)
                    with self._timed('commit_seconds'):
                        connection.commit()
                    self.rows_written += len(values)
        return 0, None

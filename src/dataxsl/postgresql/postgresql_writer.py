"""PostgreSQL sink for the one-reader, N-writer multiprocessing pipeline."""
from contextlib import contextmanager
import logging
from time import perf_counter
from typing import Any

import pandas as pd
import psycopg
from psycopg import sql

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
DB_CONFIG_SCHEMA = {
    'type': 'object', 'minProperties': 1, 'additionalProperties': False,
    'not': {'required': ['database', 'dbname']},
    'properties': {
        **{name: {'type': 'string'} for name in (
            'host', 'hostaddr', 'user', 'password', 'database', 'dbname',
            'application_name', 'options', 'client_encoding',
            'sslrootcert', 'sslcert', 'sslkey')},
        'port': {'type': 'integer', 'minimum': 1, 'maximum': 65535},
        'schema': {'type': 'string', 'minLength': 1,
                   'allOf': [{'pattern': r'\S'}, {'pattern': r'^[^\x00]+$'}]},
        'connect_timeout': {'type': 'integer', 'minimum': 1},
        'autocommit': {'type': ['boolean', 'null'], 'enum': [False, None]},
        'sslmode': {'enum': ['disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full']},
        'keepalives': {'type': 'integer', 'enum': [0, 1]},
        **{name: {'type': 'integer', 'minimum': 0} for name in (
            'keepalives_idle', 'keepalives_interval', 'keepalives_count', 'tcp_user_timeout')},
    },
}
POSTGRESQL_WRITER_SCHEMA = {
    'type': 'object', 'required': ['db_config', 'table', 'column'],
    'additionalProperties': False,
    'properties': {
        'column': {'type': 'array', 'minItems': 1, 'uniqueItems': True,
                   'items': {'type': 'string', 'minLength': 1, 'pattern': r'^[^\x00]+$'}},
        'writerMode': {'type': 'string', 'const': 'insert'},
        'pre_sql': SQL_SCHEMA, 'post_sql': SQL_SCHEMA, 'session': SQL_SCHEMA,
        'additive_attr': {'type': ['object', 'null'], 'propertyNames': {'type': 'string'},
                          'additionalProperties': {'type': ['string', 'number', 'boolean', 'null']}},
        'column_types': COLUMN_TYPES_SCHEMA,
        'db_config': DB_CONFIG_SCHEMA,
        # A name, or schema.name. Each part is quoted literally below.
        'table': {'type': 'string', 'pattern': r'^[^.\x00]+(?:\.[^.\x00]+)?$'},
    },
}


def sql_list(value):
    if value is None:
        return []
    return [value] if isinstance(value, str) else value


def identifier(*parts):
    # Escape literal percent signs for the driver's %s placeholder parser too.
    return sql.Identifier(*parts).as_string().replace('%', '%%')


@PluginRegistry.register_writer('postgresql_writer')
class PostgreSQLWriter(Writer):
    schema: dict[str, Any] = POSTGRESQL_WRITER_SCHEMA

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
        self.db_config['autocommit'] = False
        self.db_config.setdefault('connect_timeout', 10)
        if 'database' in self.db_config:
            self.db_config['dbname'] = self.db_config.pop('database')
        if 'password' in self.db_config:
            self.db_config['password'] = resolve_secret(self.db_config['password'])
        table_parts = self.table.split('.')
        if len(table_parts) == 1 and 'schema' in self.db_config:
            table_parts.insert(0, self.db_config['schema'])
        table = identifier(*table_parts)
        columns = ', '.join(identifier(column) for column in self.column)
        placeholders = ', '.join(['%s'] * len(self.column))
        self.sql_insert = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'

    def _connect(self):
        # schema is a connector option, not a libpq connection parameter.
        return psycopg.connect(**{key: value for key, value in self.db_config.items() if key != 'schema'})

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
                cursor = connection.cursor()
                if 'schema' in self.db_config:
                    # No bound parameters here: Identifier quotes the complete schema
                    # name, and literal percent signs must not be doubled.
                    search_path = sql.SQL('SET search_path TO {}').format(
                        sql.Identifier(self.db_config['schema']))
                    cursor.execute(search_path.as_string())
                for statement in self.session:
                    cursor.execute(statement)

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
                        logger.error('Failed to close PostgreSQL resource: %s', type(exc).__name__)
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
                    converted.append(convert_value(value, self.column_types.get(column)))
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
                    # logger.debug(self.sql_insert)
                    # logger.debug(values)
                    # debug_value = values[0]
                    # logger.debug(debug_value)
                    with self._timed('execute_seconds'):
                        cursor.executemany(self.sql_insert, values)
                    with self._timed('commit_seconds'):
                        connection.commit()
                    self.rows_written += len(values)
        return 0, None

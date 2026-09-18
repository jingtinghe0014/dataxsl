from contextlib import contextmanager
import inspect
import logging
from time import perf_counter

import pandas as pd
import pymysql

from dataxsl.register import PluginRegistry
from dataxsl.config import validate_schema
from dataxsl.transform import convert_value, COLUMN_TYPES_SCHEMA
from dataxsl.utils import resolve_secret
from dataxsl.writer import Writer

logger = logging.getLogger(__name__)
CONNECTION_PARAMETERS = set(inspect.signature(pymysql.connect).parameters)

SQL_SCHEMA = {'anyOf': [
    {'type': 'null'}, {'type': 'string', 'pattern': r'\S'},
    {'type': 'array', 'items': {'type': 'string', 'pattern': r'\S'}},
]}
DB_CONFIG_SCHEMA = {
    'type': 'object', 'minProperties': 1, 'additionalProperties': False,
    'properties': {
        **{name: {} for name in CONNECTION_PARAMETERS},
        **{name: {'type': ['string', 'null']} for name in
           ('host', 'user', 'database', 'db', 'charset', 'unix_socket', 'bind_address')},
        **{name: {'type': 'string'} for name in ('password', 'passwd')},
        'port': {'type': 'integer', 'minimum': 1, 'maximum': 65535},
        'autocommit': {'type': ['boolean', 'null'], 'enum': [False, None]},
        **{name: {'type': 'number', 'exclusiveMinimum': 0} for name in
           ('connect_timeout', 'read_timeout', 'write_timeout')},
    },
}
MYSQL_WRITER_SCHEMA = {
    'type': 'object', 'required': ['db_config', 'table', 'column'],
    'additionalProperties': False,
    'properties': {
        'db_config': DB_CONFIG_SCHEMA,
        'table': {'type': 'string', 'pattern': r'^[^.\x00]+(?:\.[^.\x00]+)*$'},
        'column': {'type': 'array', 'minItems': 1, 'uniqueItems': True,
                   'items': {'type': 'string', 'minLength': 1, 'pattern': r'^[^\x00]+$'}},
        'writerMode': {'type': 'string', 'const': 'insert'},
        'pre_sql': SQL_SCHEMA, 'post_sql': SQL_SCHEMA, 'session': SQL_SCHEMA,
        'additive_attr': {'type': ['object', 'null'], 'propertyNames': {'type': 'string'},
                          'additionalProperties': {'type': ['string', 'number', 'boolean', 'null']}},
        'column_types': COLUMN_TYPES_SCHEMA,
    },
}


def sql_list(value):
    if value is None:
        return []
    return [value] if isinstance(value, str) else value


def identifier(value):
    if not isinstance(value, str) or not value or '\x00' in value:
        raise ValueError('SQL identifiers must be nonempty strings')
    return '`' + value.replace('`', '``') + '`'


@PluginRegistry.register_writer('mysql_writer')
class MySQLWriter(Writer):
    schema = MYSQL_WRITER_SCHEMA

    def __init__(self, **config):
        self._extra_config = {name: value for name, value in config.items() if name not in self.schema['properties']}
        self.db_config: dict = dict(config['db_config'])
        self.table = config.get('table')
        self.column = config.get('column', [])
        self.pre_sql = config.get('pre_sql')
        self.post_sql = config.get('post_sql')
        self.session = config.get('session', [])
        self.additive_attr = config.get('additive_attr', {})
        self.mode = config.get('writerMode', 'insert')
        self.column_types = config.get('column_types', {})
        self.rows_written = 0
        self.sql_insert = None
        self.timings = {}

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
        self.db_config['autocommit'] = False
        for name, default in [('connect_timeout', 10), ('read_timeout', 30), ('write_timeout', 30)]:
            self.db_config.setdefault(name, default)
        for name in ('password', 'passwd'):
            if name in self.db_config:
                self.db_config[name] = resolve_secret(self.db_config[name])
        table = '.'.join(identifier(part) for part in self.table.split('.'))
        self.sql_insert = f"INSERT INTO {table} ({', '.join(identifier(c) for c in self.column)}) VALUES ({', '.join(['%s'] * len(self.column))})"

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
                connection = pymysql.connect(**self.db_config)
                cursor = connection.cursor()
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
                        logger.error('Failed to close MySQL resource: %s', type(exc).__name__)
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
                    # logger.debug('sql_insert = ' + self.sql_insert)
                    # logger.debug(values)
                    with self._timed('execute_seconds'):
                        cursor.executemany(self.sql_insert, values)
                    with self._timed('commit_seconds'):
                        connection.commit()
                    self.rows_written += len(values)
        return 0, None

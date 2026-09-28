"""Paginated JSON source; HTTP sessions live only in the reader process."""
import codecs
from contextlib import contextmanager
from copy import deepcopy
import math
from time import monotonic, perf_counter, sleep
from typing import Any
from urllib.parse import parse_qsl, quote, urlsplit
import logging


import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict

from dataxsl.config import validate_schema
from dataxsl.reader import Reader
from dataxsl.register import PluginRegistry
from dataxsl.runtime import ProcessingCancelled
from dataxsl.transform import TYPES, convert_value
from dataxsl.utils import resolve_secret

logger = logging.getLogger(__name__)

PATH_SCHEMA = {'type': 'string', 'pattern': r'^[^.\s]+(?:\.[^.\s]+)*$'}
AUTH_SCHEMA = {
    'type': 'object', 'additionalProperties': False,
    'required': ['auth_type', 'auth_url', 'auth_username', 'auth_password'],
    'properties': {
        'auth_type': {'const': 'basic'}, 'auth_url': {'type': 'string', 'minLength': 1},
        'method': {'enum': ['GET', 'POST']},
        'auth_username': {'type': 'string', 'minLength': 1},
        'auth_password': {'type': 'string'},
        'token_path': PATH_SCHEMA,
        'token_header': {'type': 'string', 'pattern': r'^[A-Za-z0-9-]+$'},
        'token_prefix': {'type': 'string', 'pattern': r'^[^\r\n]*$'},
    },
}
RESTAPI_READER_SCHEMA = {
    'type': 'object', 'required': ['url'], 'additionalProperties': False,
    'properties': {
        'response_type': {'const': 'json'}, 'url': {'type': 'string', 'minLength': 1},
        'method': {'enum': ['GET', 'POST']},
        'headers': {'type': 'object', 'propertyNames': {'pattern': r'^[A-Za-z0-9-]+$'},
                    'additionalProperties': {'type': 'string', 'pattern': r'^[^\r\n]*$'}},
        'Authorization': {'anyOf': [{'type': 'null'}, AUTH_SCHEMA]},
        'json_path': PATH_SCHEMA,
        'batch_size': {'type': 'integer', 'minimum': 1},
        'encode': {'type': 'string', 'minLength': 1},
        'read_timeout': {'type': 'integer', 'minimum': 1},
        'requestParam': {'anyOf': [{'type': 'string'}, {'type': 'object'}]},
        'pagination': {'anyOf': [{'type': 'null'}, {
            'type': 'object', 'additionalProperties': False,
            'required': ['total', 'pageSize', 'pageNum'],
            'properties': {name: PATH_SCHEMA for name in ('total', 'pageSize', 'pageNum')},
        }]},
        'max_retry': {'type': 'integer', 'minimum': 0},
        'backoff_factor': {'type': 'number', 'minimum': 0},
        'column': {'type': 'array', 'items': {
            'type': 'object', 'required': ['index'], 'additionalProperties': False,
            'properties': {
                'index': {'anyOf': [{'type': 'integer', 'minimum': 0},
                                    {'type': 'string', 'pattern': r'^[0-9]+$'}]},
                'type': {'enum': sorted(TYPES)}, 'format': {'type': 'string'},
            },
        }},
    },
}


def json_node(document, path):
    value = document
    for name in path.split('.'):
        if not isinstance(value, dict) or name not in value:
            raise ValueError(f'REST response is missing JSON node: {path}')
        value = value[name]
    return value


def page_integer(value, name, minimum=0):
    if isinstance(value, str) and value.isascii() and value.isdigit():
        value = int(value)
    if type(value) is not int or value < minimum:
        raise ValueError(f'REST pagination {name} must be an integer >= {minimum}')
    return value


@PluginRegistry.register_reader('restapi_reader')
class RestAPIReader(Reader):
    schema: dict[str, Any] = RESTAPI_READER_SCHEMA

    def __init__(self, **config: Any):
        self.config: dict[str, Any] = {
            'response_type': 'json', 'method': 'GET', 'headers': {}, 'Authorization': None,
            'json_path': 'data.data', 'batch_size': 1000, 'encode': 'utf-8',
            'read_timeout': 60000, 'requestParam': {}, 'pagination': None,
            'max_retry': 3, 'backoff_factor': 1, 'column': [], **deepcopy(config),
        }
        self._session: requests.Session | None = None
        self._password: str | None = None
        self._stop: Any = None
        self._columns: list[Any] | None = None
        self._source_columns: list[Any] = []
        self._row_kind: type | None = None
        self._page: list[Any] | None = None
        self._token: str | None = None
        self._token_deadline: float | None = None
        self._total: int | None = None
        self._page_size: int | None = None
        self._page_num = 1
        self._params: dict[str, Any] = {}
        self.pre_timings: dict[str, float] = {}
        self.init_timings: dict[str, float] = {}
        self.timings: dict[str, float] = {}

    def validate(self):
        validate_schema(self.config, self.schema, 'reader.parameter')
        for name, value in [('url', self.config['url']),
                            *([('Authorization.auth_url', self.config['Authorization']['auth_url'])]
                              if self.config['Authorization'] else [])]:
            try:
                parsed = urlsplit(value)
                valid = (parsed.scheme in ('http', 'https') and parsed.hostname
                         and parsed.username is None and parsed.password is None and not parsed.fragment)
                _ = parsed.port  # Validate malformed port syntax without making a connection.
            except ValueError:
                valid = False
            if not valid:
                raise ValueError(f'Invalid configuration at reader.parameter.{name} (HTTP URL)')
        auth = self.config['Authorization']
        if auth and ('{' in auth['auth_url'] or '}' in auth['auth_url']):
            parsed = urlsplit(auth['auth_url'])
            remainder = parsed.path.replace('{auth_username}', '').replace('{auth_password}', '')
            if (any(c in parsed.netloc + parsed.query for c in '{}')
                    or any(c in remainder for c in '{}')
                    or not all(marker in parsed.path for marker in ('{auth_username}', '{auth_password}'))):
                raise ValueError('Authorization.auth_url requires both credential placeholders in the URL path')
        try:
            codecs.lookup(self.config['encode'])
        except LookupError:
            raise ValueError('Invalid configuration at reader.parameter.encode') from None
        columns = self.config['column']
        if len({int(column['index']) for column in columns}) != len(columns):
            raise ValueError('reader.parameter.column contains duplicate indexes')
        if any('format' in column and 'type' not in column for column in columns):
            raise ValueError('reader.parameter.column format requires type')
        self._request_params()  # Catch invalid paging inputs before writer pre_sql.

    def _request_params(self):
        raw = self.config['requestParam']
        params: dict[str, Any] = dict(parse_qsl(raw, keep_blank_values=True)) if isinstance(raw, str) else deepcopy(raw)
        pagination = self.config['pagination']
        if pagination:
            num_key, size_key = (pagination[name].split('.')[-1] for name in ('pageNum', 'pageSize'))
            if num_key == size_key:
                raise ValueError('REST pagination pageNum and pageSize must have different request names')
            params[num_key] = page_integer(params.get(num_key, 1), 'pageNum', 1)
            params[size_key] = page_integer(params.get(size_key, self.config['batch_size']), 'pageSize', 1)
        return params

    def pre_deal(self):
        auth = self.config['Authorization']
        if auth:
            self._password = resolve_secret(auth['auth_password'])

    def post_deal(self):
        pass

    def output_columns(self):
        return self._columns

    @contextmanager
    def _timed(self, stage):
        started = perf_counter()
        try:
            yield
        finally:
            self.timings[stage] = self.timings.get(stage, 0.0) + perf_counter() - started

    def _check_cancelled(self):
        if self._stop is not None and self._stop.is_set():
            raise ProcessingCancelled('Job cancelled')

    def _backoff(self, retry):
        self._check_cancelled()
        # Limit individual waits to one minute, including very large retry counts.
        factor = self.config['backoff_factor']
        delay = min(60.0, min(60.0, factor) * (2.0 ** min(retry, 1023))) if factor else 0.0
        with self._timed('backoff_seconds'):
            if self._stop is not None:
                if self._stop.wait(delay):
                    raise ProcessingCancelled('Job cancelled during REST retry')
            else:
                sleep(delay)

    def _request(self, method, url, *, authentication=False, **kwargs):
        if self._session is None:
            raise RuntimeError('REST reader is not prepared')
        for attempt in range(self.config['max_retry'] + 1):
            self._check_cancelled()
            try:
                with self._timed('http_seconds'):
                    response = self._session.request(method, url, timeout=self.config['read_timeout'] / 1000,
                                                     allow_redirects=False, **kwargs)
            except (requests.Timeout, requests.ConnectionError):
                if attempt == self.config['max_retry']:
                    raise RuntimeError('REST request failed after retries (connection or timeout)') from None
            except (requests.RequestException, ValueError, UnicodeError):
                raise RuntimeError('REST request failed (invalid request or transport error)') from None
            else:
                status = response.status_code
                try:
                    if status == 401 and not authentication:
                        return status, None
                    if 200 <= status < 300:
                        response.encoding = self.config['encode']
                        try:
                            return status, response.json()
                        except ValueError:
                            raise ValueError('REST response is not valid JSON') from None
                    retryable = status in (408, 429) or 500 <= status < 600
                    if not retryable or attempt == self.config['max_retry']:
                        raise RuntimeError(f'REST {"authentication" if authentication else "data"} request failed (HTTP {status})')
                finally:
                    response.close()
            self._backoff(attempt)
        raise RuntimeError('REST retry limit reached')

    def _authenticate(self):
        auth = self.config['Authorization']
        if self._password is None:
            self.pre_deal()
        assert self._password is not None
        headers = CaseInsensitiveDict(self.config['headers'])
        headers.pop(auth.get('token_header', 'Authorization'), None)
        method = auth.get('method', 'GET')
        url = auth['auth_url']
        credentials = {'username': auth['auth_username'], 'password': self._password}
        content_type = headers.get('Content-Type', 'application/json').split(';')[0].strip().lower()
        if '{auth_username}' in url or '{auth_password}' in url:
            # Encode each credential as a single path component, never as a query parameter.
            url = url.format(auth_username=quote(auth['auth_username'], safe=''),
                             auth_password=quote(self._password, safe=''))
            payload = {}
        elif method == 'GET':
            payload = {'params': credentials}
        elif content_type == 'application/json' or content_type.endswith('+json'):
            payload = {'json': credentials}
        else:
            payload = {'data': credentials}
        _, document = self._request(method, url, authentication=True,
                                    headers=dict(headers), **payload)
        candidates = [auth['token_path']] if 'token_path' in auth else ['access_token', 'token', 'data.access_token', 'data.token', 'data']
        token = document if isinstance(document, str) and 'token_path' not in auth else None
        for path in candidates:
            if token is not None:
                break
            try:
                value = json_node(document, path)
            except ValueError:
                continue
            if isinstance(value, str) and value.strip():
                token = value
        if not isinstance(token, str) or not token.strip() or '\r' in token or '\n' in token:
            raise ValueError('REST authentication response has no usable token; configure Authorization.token_path')
        self._token = token
        self._token_deadline = None
        for container in (document, document.get('data') if isinstance(document, dict) else None):
            if isinstance(container, dict) and 'expires_in' in container:
                seconds = container['expires_in']
                if type(seconds) in (int, float) and math.isfinite(seconds) and seconds > 0:
                    self._token_deadline = monotonic() + seconds
                    break

    def _fetch_page(self):
        auth = self.config['Authorization']
        for refresh in range(2):
            headers = CaseInsensitiveDict(self.config['headers'])
            if auth:
                header = auth.get('token_header', 'Authorization')
                if self._token is None and refresh == 0 and headers.get(header):
                    pass  # Try a caller-provided header before obtaining a new token.
                else:
                    if self._token is None or (self._token_deadline is not None and monotonic() >= self._token_deadline):
                        self._authenticate()
                    assert self._token is not None
                    headers[header] = auth.get('token_prefix', 'Bearer ') + self._token
            content_type = headers.get('Content-Type', 'application/json').split(';')[0].strip().lower()
            if self.config['method'] == 'GET':
                payload = {'params': deepcopy(self._params)}
            elif content_type == 'application/json' or content_type.endswith('+json'):
                payload = {'json': deepcopy(self._params)}
            else:
                payload = {'data': deepcopy(self._params)}
            status, document = self._request(self.config['method'], self.config['url'], headers=dict(headers), **payload)
            if status != 401:
                return self._parse_page(document)
            if not auth or refresh:
                raise RuntimeError('REST data request unauthorized after token refresh')
            self._token = self._token_deadline = None
        raise RuntimeError('REST token refresh failed')

    def _parse_page(self, document):
        rows = json_node(document, self.config['json_path'])
        if not isinstance(rows, list):
            raise ValueError('REST json_path must point to an array of records')
        pagination = self.config['pagination']
        if pagination:
            total = page_integer(json_node(document, pagination['total']), 'total')
            size = page_integer(json_node(document, pagination['pageSize']), 'pageSize', 1)
            page = page_integer(json_node(document, pagination['pageNum']), 'pageNum', 1)
            if page != self._page_num:
                raise ValueError('REST response pageNum differs from the requested page')
            if self._total is not None and (total != self._total or size != self._page_size):
                raise ValueError('REST total or pageSize changed during pagination')
            expected = min(size, max(0, total - (page - 1) * size))
            if len(rows) != expected:
                raise ValueError('REST page row count disagrees with pagination metadata')
            self._total, self._page_size = total, size
        return rows

    def _set_columns(self, rows):
        configured = self.config['column']
        if rows:
            first = rows[0]
            if isinstance(first, dict):
                self._row_kind = dict
                self._source_columns = list(first)
            elif isinstance(first, list):
                self._row_kind = list
                self._source_columns = list(range(len(first)))
            else:
                raise ValueError('REST records must be objects or arrays')
            indexes = [int(c['index']) for c in configured] if configured else list(range(len(first)))
            if not indexes or max(indexes) >= len(first):
                raise ValueError('REST column index exceeds source width or source row is empty')
            self._columns = [self._source_columns[i] for i in indexes]
        else:
            self._columns = [int(c['index']) for c in configured] or None

    def prepare_read(self):
        if self._session is not None:
            raise RuntimeError('REST reader is already prepared')
        started = perf_counter()
        self.timings = {}
        self._params = self._request_params()
        self._total = self._page_size = None
        self._token = self._token_deadline = None
        self._source_columns = []
        self._columns = self._row_kind = None
        pagination = self.config['pagination']
        self._page_num = self._params[pagination['pageNum'].split('.')[-1]] if pagination else 1
        self._session = requests.Session()
        self._session.trust_env = False  # Never replace the configured token using ~/.netrc.
        try:
            self._page = self._fetch_page()
            self._set_columns(self._page)
        except BaseException:
            self.cleanup()
            raise
        finally:
            self.init_timings = {'prepare_seconds': perf_counter() - started}

    def _values(self, row):
        if type(row) is not self._row_kind:
            raise ValueError('REST record shape changed')
        if isinstance(row, dict):
            if set(row) != set(self._source_columns):
                raise ValueError('REST record fields changed')
            values = [row[key] for key in self._source_columns]
        elif isinstance(row, list):
            if len(row) != len(self._source_columns):
                raise ValueError('REST record width changed')
            values = row
        else:
            raise ValueError('REST records must be objects or arrays')
        selected = []
        for column in self.config['column'] or [{'index': i} for i in range(len(values))]:
            value = values[int(column['index'])]
            if isinstance(value, (dict, list)):
                raise ValueError('REST record cells must be scalar values')
            try:
                selected.append(convert_value(value, column if 'type' in column else None))
            except Exception:
                raise ValueError(f"REST field conversion failed at column index {column['index']}") from None
        return selected

    def read_parallel(self, queue):
        self._stop = getattr(queue, 'stop', None)
        # logger.debug("")
        try:
            if self._session is None:
                self.prepare_read()
            batch = []
            while True:
                assert self._page is not None
                for row in self._page:
                    self._check_cancelled()
                    batch.append(self._values(row))
                    if len(batch) == self.config['batch_size']:
                        self._enqueue(queue, batch)
                        batch = []
                self._page = None
                if not self.config['pagination']:
                    break
                assert self._page_size is not None and self._total is not None
                if self._page_num * self._page_size >= self._total:
                    break
                self._page_num += 1
                pagination = self.config['pagination']
                self._params[pagination['pageNum'].split('.')[-1]] = self._page_num
                self._params[pagination['pageSize'].split('.')[-1]] = self._page_size
                self._page = self._fetch_page()
            if batch:
                self._enqueue(queue, batch)
            return 0, None
        finally:
            self.cleanup()

    def _enqueue(self, queue, batch):
        with self._timed('dataframe_seconds'):
            frame = pd.DataFrame(batch, columns=self._columns, dtype=object)
        with self._timed('enqueue_seconds'):
            queue.put(frame)

    def cleanup(self):
        session, self._session = self._session, None
        self._page = self._token = self._token_deadline = None
        if session is not None:
            session.close()

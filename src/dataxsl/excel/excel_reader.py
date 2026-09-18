import logging
import math
import os
from pathlib import Path
import tempfile
from contextlib import contextmanager
from itertools import islice
from time import perf_counter

import msoffcrypto
import pandas as pd
from python_calamine import CalamineWorkbook

from dataxsl.config import validate_schema
from dataxsl.reader import Reader
from dataxsl.register import PluginRegistry
from dataxsl.utils import resolve_secret

logger = logging.getLogger(__name__)

EXCEL_READER_SCHEMA = {
    'type': 'object', 'required': ['path', 'sheet_name'], 'additionalProperties': False,
    'properties': {
        'path': {'type': 'string', 'minLength': 1},
        'sheet_name': {'type': 'string', 'minLength': 1},
        'exl_password': {'type': ['string', 'null']},
        'chunk_size': {'type': 'integer', 'minimum': 1},
        'header': {'type': 'integer', 'minimum': 0},
        'ins_row_num': {'type': 'boolean'},
        'skip_rows': {'anyOf': [
            {'type': 'null'}, {'type': 'integer', 'minimum': 0},
            {'type': 'array', 'items': {'type': 'integer', 'minimum': 0}}]},
        'use_cols': {'anyOf': [
            {'type': 'null'}, {'type': 'array', 'minItems': 1, 'uniqueItems': True,
                              'items': {'type': 'integer', 'minimum': 0}}]},
        # Retained legacy options do not control the reader engine.
        'parallel': {}, 'encode': {},
    },
}


@PluginRegistry.register_reader('excel_reader')
class ExcelReader(Reader):
    schema = EXCEL_READER_SCHEMA

    def __init__(self, path=None, sheet_name=None, exl_password=None, chunk_size=1000,
                 parallel=1, header=0, skip_rows=None, use_cols=None, ins_row_num=False,
                 encode='utf-8', **extra_config):
        self._extra_config = extra_config
        self.source_path = path
        self.file_path = path
        self.sheet_name = sheet_name
        self.exl_password = exl_password
        self.chunk_size = chunk_size
        self.parallel = parallel  # Legacy constructor compatibility; engine owns END.
        self.header = header
        self.skip_rows = skip_rows
        self.use_cols = use_cols
        self.ins_row_num = ins_row_num
        self.encode = encode
        self._temporary_path = None
        self._columns = None
        self.pre_timings = {}
        self.init_timings = {}
        self.timings = {}
        self._workbook = None
        self._rows = None
        self._header = None
        self._runtime_directory = None

    @staticmethod
    @contextmanager
    def _timed(timings, stage):
        started = perf_counter()
        try:
            yield
        finally:
            timings[stage] += perf_counter() - started

    def validate(self):
        config = {'path': os.fspath(self.source_path) if isinstance(self.source_path, os.PathLike) else self.source_path,
                  'sheet_name': self.sheet_name, 'exl_password': self.exl_password,
                  'chunk_size': self.chunk_size, 'header': self.header,
                  'ins_row_num': self.ins_row_num, 'skip_rows': self.skip_rows,
                  'use_cols': self.use_cols, 'parallel': self.parallel, 'encode': self.encode,
                  **self._extra_config}
        validate_schema(config, self.schema, 'reader.parameter')
        if not Path(self.source_path).is_file():
            raise ValueError('reader.path must name an existing file')

    def pre_deal(self):
        self.pre_timings = {'decrypt_seconds': 0.0}
        if self.exl_password:
            with self._timed(self.pre_timings, 'decrypt_seconds'):
                self.exl_password = resolve_secret(self.exl_password)
                self.file_path = self._decrypt_excel()

    def prepare_read(self):
        if self._workbook is not None:
            raise RuntimeError('Excel reader is already prepared')
        self.init_timings = dict.fromkeys(('open_seconds', 'sheet_seconds',
                                         'header_seconds', 'close_seconds'), 0.0)
        success = False
        try:
            # Keep this workbook and iterator for reading immediately after checks.
            with self._timed(self.init_timings, 'open_seconds'):
                self._workbook = CalamineWorkbook.from_path(self.file_path)
            with self._timed(self.init_timings, 'sheet_seconds'):
                sheet = self._workbook.get_sheet_by_name(self.sheet_name)
            with self._timed(self.init_timings, 'header_seconds'):
                self._rows = enumerate(sheet.iter_rows())
                for index, row in self._rows:
                    if index == self.header:
                        self._header = list(row)
                        self._columns = self._select_columns(self._header)
                        break
                else:
                    raise ValueError('Configured header row does not exist')
            success = True
        finally:
            if not success:
                with self._timed(self.init_timings, 'close_seconds'):
                    self._close_workbook()

    def _select_columns(self, header):
        columns = (['IDX'] if self.ins_row_num else []) + header
        if self.use_cols is not None:
            if max(self.use_cols, default=0) >= len(columns):
                raise ValueError('use_cols index exceeds source column count')
            columns = [columns[index] for index in self.use_cols]
        return columns

    def output_columns(self):
        return self._columns

    def post_deal(self):
        pass

    def configure_runtime(self, directory):
        self._runtime_directory = directory

    def _close_workbook(self):
        workbook = self._workbook
        self._workbook = self._rows = self._header = None
        if workbook is not None:
            workbook.close()

    def cleanup(self):
        try:
            self._close_workbook()
        finally:
            if self._temporary_path is not None:
                Path(self._temporary_path).unlink(missing_ok=True)
                self._temporary_path = None
            self.file_path = self.source_path

    def _decrypt_excel(self):
        descriptor, path = tempfile.mkstemp(prefix='dataxsl-', suffix=Path(self.source_path).suffix,
                                          dir=self._runtime_directory)
        os.close(descriptor)
        self._temporary_path = path
        with open(self.source_path, 'rb') as source, open(path, 'wb') as destination:
            office_file = msoffcrypto.OfficeFile(source)
            office_file.load_key(password=self.exl_password)
            office_file.decrypt(destination)
        return path

    @staticmethod
    def _convert_cell(value):
        if value is None or (isinstance(value, str) and value == ''):
            return None
        if isinstance(value, float):
            if math.isnan(value):
                return None
            if math.isfinite(value) and value.is_integer():
                return int(value)
        return value

    def read_parallel(self, queue):
        self.timings = dict.fromkeys(('rows_seconds', 'dataframe_seconds',
                                     'enqueue_seconds', 'close_seconds'), 0.0)
        try:
            # Standalone callers may omit prepare_read; workers initialize before reading.
            if self._workbook is None:
                self.prepare_read()

            skip = {self.skip_rows} if type(self.skip_rows) is int else set(self.skip_rows or [])
            records = self._iter_records(self._rows, self._header, skip)
            while True:
                # Time row iteration/conversion once per batch, not once per cell or row.
                with self._timed(self.timings, 'rows_seconds'):
                    chunk = list(islice(records, self.chunk_size))
                if not chunk:
                    break
                with self._timed(self.timings, 'dataframe_seconds'):
                    frame = pd.DataFrame(chunk, columns=self._columns, dtype=object)
                with self._timed(self.timings, 'enqueue_seconds'):
                    queue.put(frame)
            return 0, None
        finally:
            with self._timed(self.timings, 'close_seconds'):
                self._close_workbook()

    def _iter_records(self, rows, header, skip):
        record_index = 0
        for index, row in rows:
            if index in skip:
                continue
            cells = [self._convert_cell(cell) for cell in row]
            if len(cells) != len(header):
                raise ValueError('Source row width differs from header')
            selected = ([record_index + 1] if self.ins_row_num else []) + cells
            if self.use_cols is not None:
                selected = [selected[i] for i in self.use_cols]
            # Exclude the synthetic IDX when testing whether the business row is empty.
            business_indexes = [i for i in (self.use_cols if self.use_cols is not None
                                            else range(len(cells) + int(self.ins_row_num)))
                                if not (self.ins_row_num and i == 0)]
            original = ([None] if self.ins_row_num else []) + cells
            if not any(original[i] is not None for i in business_indexes):
                continue
            record_index += 1
            yield selected

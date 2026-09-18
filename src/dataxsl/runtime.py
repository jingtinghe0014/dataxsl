"""Spawn-safe workers and cancellable process queue operations."""
from queue import Empty, Full
import os
import logging
from time import perf_counter, process_time
from typing import Any, Literal, TypedDict
from dataxsl.logging_config import LoggingManager


WorkerRole = Literal['reader', 'writer']


class _WorkerTiming(TypedDict):
    role: WorkerRole
    pid: int
    elapsed_seconds: float
    queue_seconds: float
    active_seconds: float
    initialization_seconds: float
    processing_seconds: float
    cpu_seconds: float
    batches: int
    stages: dict[str, float]


class WorkerTiming(_WorkerTiming, total=False):
    # Added by the parent after receiving the report.
    process_name: str
    rows: int


class _WorkerResult(TypedDict):
    kind: Literal['result']
    rows_read: int
    rows_written: int
    init_timings: dict[str, float]
    timing: WorkerTiming


class WorkerSuccess(_WorkerResult):
    ok: Literal[True]


class WorkerFailure(_WorkerResult):
    ok: Literal[False]
    error_type: str
    message: str


WorkerResult = WorkerSuccess | WorkerFailure


class ProcessingCancelled(Exception):
    pass


class WorkerError(RuntimeError):
    pass


class CancellableQueue:
    """Keep the plugin queue API while periodically observing cancellation."""
    def __init__(self, queue, stop):
        self.queue = queue
        self.stop = stop
        self.rows_read = 0
        self.finished = False
        self.queue_seconds = 0.0
        self.batches = 0

    def put(self, item):
        started = perf_counter()
        try:
            self._put(item)
            return
        finally:
            self.queue_seconds += perf_counter() - started

    def _put(self, item):
        while not self.stop.is_set():
            try:
                self.queue.put(item, timeout=0.1)
                if item is not None:
                    self.rows_read += len(item)
                    self.batches += 1
                return
            except Full:
                continue
        raise ProcessingCancelled('Job cancelled')

    def get(self):
        started = perf_counter()
        try:
            return self._get()
        finally:
            self.queue_seconds += perf_counter() - started

    def _get(self):
        while not self.stop.is_set():
            try:
                item = self.queue.get(timeout=0.1)
                if item is None:
                    self.finished = True
                else:
                    self.batches += 1
                return item
            except Empty:
                continue
        raise ProcessingCancelled('Job cancelled')


def worker_entry(role, plugin, queue, stop, writers, result_pipe, log_queue, log_level,
                 initialize_reader=False, resource_directory=None, validate_columns=None):
    LoggingManager.configure_worker(log_queue, log_level)
    channel = CancellableQueue(queue, stop)
    started, cpu_started = perf_counter(), process_time()
    report: dict[str, Any] = {'ok': False}
    initialization_seconds = processing_seconds = 0.0
    managed_reader = role == 'reader' and initialize_reader
    try:
        if managed_reader:
            init_started = perf_counter()
            try:
                plugin.configure_runtime(resource_directory)
                plugin.prepare_read()
                columns = plugin.output_columns()
                if columns is not None and validate_columns is not None:
                    validate_columns(columns)
            finally:
                initialization_seconds = perf_counter() - init_started
            if stop.is_set():
                raise ProcessingCancelled('Job cancelled during reader initialization')
        method = plugin.read_parallel if role == 'reader' else plugin.write_parallel
        processing_started = perf_counter()
        try:
            result = method(channel)
        finally:
            processing_seconds = perf_counter() - processing_started
        if not isinstance(result, tuple) or len(result) != 2 or type(result[0]) is not int:
            raise TypeError(f'{role} must return (integer status, error)')
        status, error = result
        if status != 0 or error is not None:
            if isinstance(error, BaseException):
                raise error
            raise RuntimeError(f'{role} returned a failure status')
        if role == 'writer' and not channel.finished:
            raise RuntimeError('Writer exited before consuming the completion signal')
        # The engine owns completion; readers must never send their own sentinels.
        if role == 'reader':
            for _ in range(writers):
                channel.put(None)
        report = {'ok': True}
    except BaseException as exc:
        report = {'ok': False, 'error_type': type(exc).__name__, 'message': str(exc)}
    finally:
        if managed_reader:
            try:
                plugin.cleanup()
            except BaseException as exc:
                logging.getLogger(__name__).error('Reader cleanup failed: %s', type(exc).__name__)
                if report['ok']:
                    report = {'ok': False, 'error_type': type(exc).__name__, 'message': str(exc)}
        elapsed = perf_counter() - started
        report.update(kind='result', rows_read=channel.rows_read,
                      rows_written=getattr(plugin, 'rows_written', 0),
                      init_timings=dict(getattr(plugin, 'init_timings', {})),
                      timing={'role': role, 'pid': os.getpid(),
                              'elapsed_seconds': elapsed,
                              'queue_seconds': channel.queue_seconds,
                              'active_seconds': max(0.0, elapsed - channel.queue_seconds),
                              'initialization_seconds': initialization_seconds,
                              'processing_seconds': processing_seconds,
                              'cpu_seconds': process_time() - cpu_started,
                              'batches': channel.batches,
                              'stages': dict(getattr(plugin, 'timings', {}))})
        try:
            result_pipe.send(report)
        finally:
            if not report['ok']:
                stop.set()
            result_pipe.close()

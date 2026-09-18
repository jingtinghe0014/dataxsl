"""One reader process and N writer processes, with bounded failure cleanup."""
import logging
import math
import multiprocessing as mp
import time
import tempfile
from contextlib import contextmanager
from logging.handlers import QueueListener
from multiprocessing.connection import Connection, wait
from multiprocessing.process import BaseProcess
from multiprocessing.synchronize import Event
from typing import Any, Literal, TypedDict, cast

from dataxsl.config import JOB_SCHEMA, load_job, positive_integer, validate_job
from dataxsl.logging_config import LoggingManager
from dataxsl.register import PluginRegistry
from dataxsl.reader import Reader
from dataxsl.writer import Writer
from dataxsl.runtime import WorkerError, WorkerResult, WorkerRole, WorkerTiming, worker_entry
from dataxsl.utils import LEGACY_KEY as key, redact

logger = logging.getLogger(__name__)


class _JobSummary(TypedDict):
    status: Literal['success']
    reader_processes: int
    writer_processes: int
    rows_read: int
    rows_written: int
    worker_timings: list[WorkerTiming]


class JobResult(_JobSummary, total=False):
    # Filled after the pipeline and final cleanup have completed.
    elapsed_seconds: float
    phase_timings: dict[str, float]
    reader_pre_timings: dict[str, float]
    reader_init_timings: dict[str, float]


class DataProcessor:
    def __init__(self, reader: Reader, writer: Writer, parallel=1, queue_size=10, channel=1,
                 shutdown_timeout=5):
        self.reader = reader
        self.writer = writer
        self.parallel = positive_integer('parallel', parallel)
        self.queue_size = positive_integer('queue_size', queue_size)
        if type(channel) is not int or channel != 1:
            raise ValueError('main.py supports channel=1 only')
        if isinstance(shutdown_timeout, bool) or not isinstance(shutdown_timeout, (int, float)) or not math.isfinite(shutdown_timeout) or shutdown_timeout <= 0:
            raise ValueError('shutdown_timeout must be positive')
        self.shutdown_timeout = shutdown_timeout
        self.result: JobResult | None = None
        self.timings: dict[str, float] = {}
        self.reader_init_timings: dict[str, float] = {}

    @contextmanager
    def _timed(self, phase):
        started = time.perf_counter()
        try:
            yield
        finally:
            self.timings[phase] = time.perf_counter() - started

    def start(self) -> JobResult:
        self.result = None
        self.timings = {}
        self.reader_init_timings = {}
        started = time.monotonic()
        failed = False
        try:
            # Validate both plugins before either can execute pre-SQL.
            with self._timed('validate_seconds'):
                self.reader.validate()
                self.writer.validate()
            with self._timed('pre_seconds'):
                self.pre_deal()
            with self._timed('pipeline_seconds'):
                self.result = self.process_data()
            with self._timed('post_seconds'):
                self.post_deal()
            with self._timed('hook_seconds'):
                self.invoke_hook()
            return self.result
        except BaseException:
            failed = True
            self.result = None
            raise
        finally:
            cleanup_started = time.perf_counter()
            errors = []
            for plugin in (self.writer, self.reader):
                try:
                    plugin.cleanup()
                except Exception as exc:
                    errors.append(exc)
                    logger.error('Plugin resource cleanup failed: %s', type(exc).__name__)
            self.timings['cleanup_seconds'] = time.perf_counter() - cleanup_started
            elapsed = time.monotonic() - started
            logger.log(logging.ERROR if failed or errors else logging.INFO,
                       'Job timing: status=%s total=%.3fs',
                       'failed' if failed or errors else 'success', elapsed)
            if errors and not failed:
                self.result = None
                raise errors[0]
            if self.result is not None:
                self.result['elapsed_seconds'] = elapsed
                self.result['phase_timings'] = dict(self.timings)
                self.result['reader_pre_timings'] = dict(getattr(self.reader, 'pre_timings', {}))
                self.result['reader_init_timings'] = dict(self.reader_init_timings)

    def pre_deal(self):
        with self._timed('reader_pre_seconds'):
            self.reader.pre_deal()
        with self._timed('writer_pre_seconds'):
            self.writer.pre_deal()

    def post_deal(self):
        self.reader.post_deal()
        self.writer.post_deal()

    def invoke_hook(self):
        pass

    def _reap(self, processes: list[BaseProcess], stop: Event) -> None:
        stop.set()
        deadline = time.monotonic() + self.shutdown_timeout
        for process in processes:
            process.join(max(0, deadline - time.monotonic()))
        alive = [process for process in processes if process.is_alive()]
        for process in alive:
            process.terminate()
        deadline = time.monotonic() + 1
        for process in alive:
            process.join(max(0, deadline - time.monotonic()))
        for process in alive:
            if process.is_alive():
                process.kill()
        for process in alive:
            process.join(1)

    def process_data(self) -> JobResult:

        # Public Process APIs allow bounded termination on Python 3.10+.
        # ProcessPoolExecutor on these versions cannot stop an already running task.
        context = mp.get_context('spawn')
        processes: list[BaseProcess] = []
        pipes: list[Connection] = []
        LoggingManager.setup_logging()
        db_config = getattr(self.writer, 'db_config', {}) or {}
        secrets = [getattr(self.reader, 'exl_password', None),
                   db_config.get('password'), db_config.get('passwd')]
        # Parent owns this directory, so forced termination cannot leak decrypted files.
        with tempfile.TemporaryDirectory(prefix='dataxsl-run-') as resources, context.Manager() as manager:
            queue = manager.Queue(self.queue_size)
            logs = manager.Queue()
            stop = context.Event()
            listener = QueueListener(logs, *logging.getLogger().handlers,
                                     respect_handler_level=True)
            listener.start()
            try:
                pending: dict[Connection, BaseProcess] = {}
                def spawn(role: WorkerRole, plugin: Reader | Writer, index: int) -> None:
                    receive, send = context.Pipe(duplex=False)
                    pipes.append(receive)
                    process = context.Process(
                        name=f'dataxsl-{role}-{index}', target=worker_entry,
                        args=(role, plugin, queue, stop, self.parallel, send, logs,
                              logging.getLogger().level,
                              role == 'reader', resources,
                              self.writer.validate_columns if role == 'reader' else None))
                    try:
                        process.start()
                    finally:
                        send.close()
                    processes.append(process)
                    pending[receive] = process
                spawn('reader', self.reader, 0)
                for index in range(1, self.parallel + 1):
                    spawn('writer', self.writer, index)
                results: list[WorkerResult] = []
                while pending:
                    # wait returns members of its input. Here every member is a Pipe,
                    # although typeshed also permits sockets and integer handles.
                    ready = cast(list[Connection], wait(list(pending), timeout=0.1))
                    for pipe in ready:
                        process = pending.pop(pipe)
                        try:
                            result = cast(WorkerResult, pipe.recv())
                        except EOFError:
                            raise WorkerError(f'{process.name} exited without a result') from None
                        timing = result['timing']
                        if timing['role'] == 'reader':
                            self.reader_init_timings = result['init_timings']
                            self.timings['reader_init_seconds'] = timing['initialization_seconds']
                        timing['process_name'] = process.name
                        timing['rows'] = result['rows_read'] if timing['role'] == 'reader' else result['rows_written']
                        logger.log(logging.INFO if result['ok'] else logging.ERROR,
                                   'Process timing: %s pid=%s status=%s total=%.3fs',
                                   process.name, timing['pid'], 'success' if result['ok'] else 'failed',
                                   timing['elapsed_seconds'])
                        if not result['ok']:
                            message = redact(result['message'], secrets)
                            raise WorkerError(f"{process.name}: {result['error_type']}: {message}")
                        results.append(result)
                    for process in processes:
                        if process.exitcode not in (None, 0):
                            raise WorkerError(f'{process.name} exited with code {process.exitcode}')
                # Reports are not enough: confirm the workers have actually exited.
                deadline = time.monotonic() + self.shutdown_timeout
                for process in processes:
                    process.join(max(0, deadline - time.monotonic()))
                    if process.is_alive() or process.exitcode != 0:
                        raise WorkerError(f'{process.name} did not exit successfully')
                return {'status': 'success', 'reader_processes': 1,
                        'writer_processes': self.parallel,
                        'rows_read': sum(item['rows_read'] for item in results),
                        'rows_written': sum(item['rows_written'] for item in results),
                        'worker_timings': [item['timing'] for item in results]}
            finally:
                self._reap(processes, stop)
                for pipe in pipes:
                    pipe.close()
                for process in processes:
                    process.close()
                listener.stop()


class DataXslContext:
    def __init__(self):
        self.construct_config: dict[str, str] = {}
        self.reader_config: dict[str, Any] = {}
        self.writer_config: dict[str, Any] = {}
        self.process_config: dict[str, int] = {'channel': 1, 'queue_size': 10, 'parallel': 1}
        self.schema = JOB_SCHEMA
        self.result: JobResult | None = None

    @staticmethod
    def _validate(config_data):
        validate_job(config_data)

    def load(self, arguments):
        self.result = None
        config = load_job(arguments.job, getattr(arguments, 'param', None))
        content = config['job']['content'][0]
        self.construct_config = {kind: content[kind]['name'] for kind in ('reader', 'writer')}
        self.reader_config = content['reader']['parameter'].copy()
        self.writer_config = content['writer']['parameter'].copy()
        self.process_config = config['job']['setting'].copy()
        # Legacy Excel metadata was never applied; do not silently start converting it.
        if 'column' in self.reader_config:
            self.reader_config.pop('column')
            logger.warning('reader.column is legacy metadata; values are written in reader output order')
        self.reader_config.pop('parallel', None)
        return config

    def parse(self, arguments):
        self.load(arguments)
        self.__construct__()
        return 0, None

    def __construct__(self):
        reader = PluginRegistry.get_reader(self.construct_config['reader'])(**self.reader_config)
        writer = PluginRegistry.get_writer(self.construct_config['writer'])(**self.writer_config)
        self.result = DataProcessor(reader, writer, **self.process_config).start()
        return self.result

import logging
import logging.config
from logging.handlers import QueueHandler
import os
from pathlib import Path

import yaml


class LoggingManager:
    """Only the parent owns file handlers; workers forward records to it."""
    _initialized = False

    @classmethod
    def setup_logging(cls, config_path=None):

        if cls._initialized:
            return
        path = Path(config_path) if config_path is not None else Path(
            f"config/logging-{os.getenv('APP_ENV', 'dev')}.yaml")

        if config_path is None and not path.exists():
            path = Path('config/logging.yaml')
        try:
            if path.exists():
                config = yaml.safe_load(path.read_text(encoding='utf-8'))
                if not isinstance(config, dict):
                    raise ValueError('Logging configuration must be an object')
                for handler in config.get('handlers', {}).values():
                    if 'filename' in handler:
                        Path(handler['filename']).parent.mkdir(parents=True, exist_ok=True)
                logging.config.dictConfig(config)
            else:
                cls._setup_fallback_logging()
        except Exception:
            cls._setup_fallback_logging()
            logging.warning('Unable to load logging configuration; using console logging')
        cls._initialized = True

    @classmethod
    def configure_worker(cls, queue, level):
        # Imports have no logging side effects. This also handles explicit fork users.
        for entry in logging.Logger.manager.loggerDict.values():
            if isinstance(entry, logging.Logger):
                entry.handlers.clear()
                entry.propagate = True
                entry.setLevel(logging.NOTSET)
        root = logging.getLogger()
        for handler in root.handlers[:]:
            root.removeHandler(handler)
            handler.close()
        root.addHandler(QueueHandler(queue))
        root.setLevel(level)
        cls._initialized = True

    @classmethod
    def _setup_fallback_logging(cls):
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - [%(processName)s PID=%(process)d] - %(name)s - %(levelname)s - %(message)s')

    @classmethod
    def get_logger(cls, name=None):
        return logging.getLogger(name)


get_logger = LoggingManager.get_logger

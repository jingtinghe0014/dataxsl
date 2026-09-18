"""DataXSL synchronous plugins. Importing the package does not open log files."""
from .logging_config import LoggingManager

__version__ = '1.1.1'
get_logger = LoggingManager.get_logger

# Import only implemented plugins; unsupported names fail registry lookup.
from .excel import excel_reader
from .mysql import mysql_writer
from .postgresql import postgresql_writer

__all__ = ['LoggingManager', 'get_logger', 'excel_reader', 'mysql_writer', 'postgresql_writer']

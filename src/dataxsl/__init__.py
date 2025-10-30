
from .logging_config import LoggingManager

# 在包导入时自动初始化日志
LoggingManager.setup_logging()

# 导出快捷函数
get_logger = LoggingManager.get_logger

from . import core
from . import register
from . import error

# 显式导入所有 reader 和 writer 模块以确保注册
from .mysql import mysql_writer
from .excel import excel_reader
from .mysql import mysql_reader
from .oracle import oracle_reader
from .excel import excel_writer
from .oracle import oracle_writer

__all__ = [
    'core',
    'register',
    'error',
    'LoggingManager',  # 导出日志函数
    'excel_reader',
    'mysql_reader',
    'oracle_reader',
    'excel_writer',
    'mysql_writer',
    'oracle_writer'
]
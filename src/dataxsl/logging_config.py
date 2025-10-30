import logging
import logging.config
import os
from pathlib import Path
import yaml


class LoggingManager:
    """统一的日志管理器"""

    _initialized = False

    @classmethod
    def setup_logging(cls, config_path=None):
        """设置全局日志配置（只执行一次）"""
        if cls._initialized:
            return

        if config_path is None:
            # 自动查找配置文件
            env = os.getenv('APP_ENV', 'dev')
            config_path = Path(f'config/logging-{env}.yaml')

            if not config_path.exists():
                config_path = Path('config/logging.yaml')

        # 确保日志目录存在
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                logging.config.dictConfig(config)
                logging.info("日志配置加载成功")
            except Exception as e:
                cls._setup_fallback_logging()
                logging.warning(f"日志配置文件加载失败，使用默认配置: {e}")
        else:
            cls._setup_fallback_logging()
            logging.warning("日志配置文件不存在，使用默认配置")

        cls._initialized = True

    @classmethod
    def _setup_fallback_logging(cls):
        """回退到默认日志配置"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    @classmethod
    def get_logger(cls, name=None):
        """获取配置好的日志记录器"""
        if not cls._initialized:
            cls.setup_logging()
        return logging.getLogger(name)


# 全局快捷方式
def get_logger(name=None):
    """获取日志记录器的快捷函数"""
    return LoggingManager.get_logger(name)
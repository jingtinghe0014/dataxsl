# 在文件顶部添加一个新的类
class PluginRegistry:
    """
    插件注册器，用于动态注册和获取 Reader 和 Writer 策略类。
    """
    _readers = {}  # 存储注册的Reader类 {'excel_reader': ExcelReader, ...}
    _writers = {}  # 存储注册的Writer类 {'mysql_writer': MySQLWriter, ...}

    @classmethod
    def register_reader(cls, name: str):
        """类装饰器，用于注册Reader"""
        def decorator(reader_cls):
            cls._readers[name] = reader_cls
            return reader_cls
        return decorator

    @classmethod
    def register_writer(cls, name: str):
        """类装饰器，用于注册Writer"""
        def decorator(writer_cls):
            cls._writers[name] = writer_cls
            return writer_cls
        return decorator

    @classmethod
    def get_reader(cls, name: str):
        """根据名称获取Reader类"""
        reader_cls = cls._readers.get(name)
        if not reader_cls:
            raise ValueError(f"Reader '{name}' is not registered. Available readers: {list(cls._readers.keys())}")
        return reader_cls

    @classmethod
    def get_writer(cls, name: str):
        """根据名称获取Writer类"""
        writer_cls = cls._writers.get(name)
        if not writer_cls:
            raise ValueError(f"Writer '{name}' is not registered. Available writers: {list(cls._writers.keys())}")
        return writer_cls

    @classmethod
    def list_readers(cls):
        """列出所有已注册的Reader"""
        return list(cls._readers.keys())

    @classmethod
    def list_writers(cls):
        """列出所有已注册的Writer"""
        return list(cls._writers.keys())

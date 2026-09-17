from multiprocessing import Queue

from dataxsl.writer import Writer


# 6. 定义具体子类 ExcelReader
class ExcelWriter(Writer):

    def __init__(self, **writer_config):
        pass

    def validate(self):
        pass

    def pre_deal(self):
        pass

    def post_deal(self):
        pass

    def write_parallel(self, queue:Queue):
        return "Writer data to Excel filer", None
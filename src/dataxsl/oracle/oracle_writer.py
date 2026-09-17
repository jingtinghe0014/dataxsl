from multiprocessing import Queue

from dataxsl.writer import Writer


# 8. 定义具体子类 OracleWriter
class OracleWriter(Writer):

    def __init__(self, **oracle_config):
        pass

    def validate(self):
        pass

    def pre_deal(self):
        pass

    def post_deal(self):
        pass

    def write_parallel(self,queue:Queue):
        return "Writing data to Oracle database", None
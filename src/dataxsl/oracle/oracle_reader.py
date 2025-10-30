from multiprocessing import Queue

from dataxsl.reader import Reader


# 4. 定义具体子类 OracleReader
class OracleReader(Reader):

    def __init__(self, **oracle_config):
        pass

    def read_parallel(self,queue:Queue):
        return "Reading data from Oracle database", None

    def validate(self):
        pass

    def pre_deal(self):
        pass

    def post_deal(self):
        pass
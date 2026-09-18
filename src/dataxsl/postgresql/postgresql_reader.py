from multiprocessing import Queue
from dataxsl.reader import Reader


# 3. 定义具体子类 PostgreSQLReader
class PostgreSQLReader(Reader):

    def __init__(self, **postgresql_config):
        pass

    def read_parallel(self,queue:Queue):
        return "Reading data from PostgreSQL database", None

    def validate(self):
        pass

    def pre_deal(self):
        pass

    def post_deal(self):
        pass
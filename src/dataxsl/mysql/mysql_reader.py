from multiprocessing import Queue
from dataxsl.reader import Reader


# 3. 定义具体子类 MySQLReader
class MySQLReader(Reader):

    def __init__(self, **mysql_config):
        pass

    def read_parallel(self,queue:Queue):
        return "Reading data from MySQL database", None

    def validate(self):
        pass

    def pre_deal(self):
        pass

    def post_deal(self):
        pass
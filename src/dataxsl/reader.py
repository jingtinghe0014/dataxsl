from abc import abstractmethod, ABC
from multiprocessing import Queue

# 1. 定义抽象基类 Reader
class Reader(ABC):
    @abstractmethod
    def read_parallel(self,queue:Queue):
        pass

    @abstractmethod
    def pre_deal(self):
        pass

    @abstractmethod
    def post_deal(self):
        pass

    @abstractmethod
    def validate(self):
        pass
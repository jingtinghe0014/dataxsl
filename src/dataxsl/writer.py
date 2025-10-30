from abc import abstractmethod, ABC
from multiprocessing import Queue

# 5. 定义抽象基类 Writer
class Writer(ABC):
    @abstractmethod
    def writer_parallel(self,queue:Queue):
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
from abc import ABC, abstractmethod


class Writer(ABC):
    @abstractmethod
    def write_parallel(self, queue):
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

    def validate_columns(self, columns):
        """Check source columns without I/O; may run in the reader child."""

    def cleanup(self):
        pass

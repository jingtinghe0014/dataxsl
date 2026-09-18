from abc import ABC, abstractmethod


class Reader(ABC):
    @abstractmethod
    def read_parallel(self, queue):
        """Produce batches; completion signals belong to the engine."""

    @abstractmethod
    def pre_deal(self):
        pass

    @abstractmethod
    def post_deal(self):
        pass

    @abstractmethod
    def validate(self):
        pass

    def output_columns(self):
        return None

    def prepare_read(self):
        """Open and inspect the source in the reader child before producing batches."""

    def configure_runtime(self, directory):
        """Set a parent-owned temporary directory before child initialization."""

    def cleanup(self):
        """Release local resources; called in the reader child and the parent."""

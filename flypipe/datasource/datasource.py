from abc import ABC, abstractmethod


class DataSource(ABC):

    @staticmethod
    @abstractmethod
    def load(*args, **kwargs):
        """
        Abstract method to load data from a data source.
        """
        pass
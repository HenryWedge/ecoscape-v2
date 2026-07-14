from abc import ABC, abstractmethod

class Query(ABC):

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def execute(self):
        pass
from abc import ABC, abstractmethod

class SloSink(ABC):

    def __init__(self, is_monitor_sink):
        self.is_monitor_sink = is_monitor_sink

    @abstractmethod
    def evaluate_slo(self, value, threshold, is_bigger_better):
        pass

    @abstractmethod
    def end_hook(self):
        pass

    def is_monitor_sink(self):
        return self.is_monitor_sink

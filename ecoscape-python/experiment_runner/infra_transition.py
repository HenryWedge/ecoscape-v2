class InfraTransition:

    def __init__(self, name: str, start: int, end: int, start_action: str, end_action: str):
        self.name = name
        self.start = start
        self.end = end
        self.start_action = start_action
        self.end_action = end_action
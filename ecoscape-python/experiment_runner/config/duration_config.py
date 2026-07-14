class ExperimentDurationConfig:

    def __init__(self, duration, load_delay, eval_delay, chaos_delay, repetitions):
        self.duration = duration
        self.load_delay = load_delay
        self.eval_delay = eval_delay
        self.chaos_delay = chaos_delay
        self.repetitions = repetitions

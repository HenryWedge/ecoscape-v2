class ExperimentDirectoryConfig:

    def __init__(self, monitor_dir, infra_dir, sut_dir, load_dir, chaos_dir):
        self.monitor_dir = monitor_dir
        self.infra_dir = infra_dir
        self.sut_dir = sut_dir
        self.load_dir = load_dir
        self.chaos_dir = chaos_dir

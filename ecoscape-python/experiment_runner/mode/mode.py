from abc import ABC

class Mode(ABC):
    def is_deploy_system(self) -> bool:
        pass

    def is_start_load(self) -> bool:
        pass

    def is_apply_infrastructure_constraints(self) -> bool:
        pass

    def is_apply_chaos(self) -> bool:
        pass


class ModeLoadTest(Mode):
    def is_deploy_system(self) -> bool:
        return False

    def is_start_load(self) -> bool:
        return True

    def is_apply_infrastructure_constraints(self) -> bool:
        return False

    def is_apply_chaos(self) -> bool:
        return False


class ModeSystemDeployer(Mode):
    def is_deploy_system(self) -> bool:
        return True

    def is_start_load(self) -> bool:
        return False

    def is_apply_infrastructure_constraints(self) -> bool:
        return False

    def is_apply_chaos(self) -> bool:
        return False


class ModeExperimentRun(Mode):
    def is_deploy_system(self) -> bool:
        return False

    def is_start_load(self) -> bool:
        return True

    def is_apply_infrastructure_constraints(self) -> bool:
        return True

    def is_apply_chaos(self) -> bool:
        return True


class ModeFullExperimentRun(Mode):
    def is_deploy_system(self) -> bool:
        return True

    def is_start_load(self) -> bool:
        return True

    def is_apply_infrastructure_constraints(self) -> bool:
        return True

    def is_apply_chaos(self) -> bool:
        return True
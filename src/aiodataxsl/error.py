
class MissingParameterError(Exception):
    def __init__(self, param_name):
        self.param_name = param_name
        super().__init__(f"The parameter '%s' is missing.", param_name)

class ParameterTypeError(Exception):
    def __init__(self, param_name):
        self.param_name = param_name
        super().__init__(f"The parameter '%s' type is error.", param_name)
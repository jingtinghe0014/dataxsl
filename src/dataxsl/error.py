class MissingParameterError(ValueError):
    def __init__(self, param_name):
        self.param_name = param_name
        super().__init__(f"The parameter '{param_name}' is missing.")


class ParameterTypeError(ValueError):
    def __init__(self, param_name):
        self.param_name = param_name
        super().__init__(f"The parameter '{param_name}' has an invalid type.")

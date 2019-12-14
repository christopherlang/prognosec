class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class IndexTypeError(Error):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, message):
        # self.expression = expression
        self.message = message


class IndexIntegrityError(Error):
    def __init__(self, message):
        self.message = message


class SeriesIntegrityError(Error):
    def __init__(self, message):
        self.message = message


class ComputeMethodError(Error):
    def __init__(self, message):
        self.message = message

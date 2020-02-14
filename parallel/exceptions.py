class BaseParallelException(Exception):
    pass


class TimeoutException(BaseParallelException):
    def __eq__(self, other):
        return type(self) == type(other)


class ParallelStatusException(BaseParallelException):
    pass
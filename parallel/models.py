import enum
import collections


@enum.unique
class ParallelStatus(enum.Enum):
    NOT_STARTED = "NOT_STARTED"
    STARTED = "STARTED"
    DONE = "DONE"
    FAILED = "FAILED"


class ParallelArg:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class _UniversalParallelParametersCollection:
    def __init__(self, params):
        self.params = params

    def iter_params(self):
        if isinstance(self.params, collections.abc.Sequence):
            yield from ((None, param) for param in self.params)
        elif isinstance(self.params, dict):
            yield from self.params.items()


class ParallelJob:
    def __init__(self, fn, name=None, args=None, kwargs=None):
        self.fn = fn
        self.name = name
        self.args = args
        self.kwargs = kwargs or {}
        self.status = ParallelStatus.NOT_STARTED
        self.future = None
        self._result = None
        # Future versions:
        # self.max_retries = -1 # math.inf
        # self.results # (prev results or failed tasks)

    def __eq__(self, other):
        return all(
            [
                self.fn == other.fn,
                self.name == other.name,
                self.args == other.args,
                self.kwargs == other.kwargs,
            ]
        )

    def __repr__(self):  # pragma: no cover
        return f"{self.__class__.__name__}({self.fn}, {self.name}, {self.args}, {self.kwargs})"

    @classmethod
    def normalize_params(cls, params, extras=None, unpack_arguments=True):
        normalized_args = []
        normalized_kwargs = (extras or {}).copy()

        if isinstance(params, ParallelArg):
            return params.args, {**normalized_kwargs, **params.kwargs}
        if not unpack_arguments:
            return (params,), normalized_kwargs
        if isinstance(params, dict):
            return tuple(), {**normalized_kwargs, **params}
        if not any([isinstance(params, list), isinstance(params, tuple)]):
            return (params,), normalized_kwargs
        for param in params:
            if isinstance(param, dict):
                normalized_kwargs.update(param)
            else:
                normalized_args.append(param)
        return tuple(normalized_args), normalized_kwargs

    @classmethod
    def normalize_job(cls, name, params, extras=None, unpack_arguments=True):
        if isinstance(params, cls):
            extras = extras or {}
            return cls(
                fn=params.fn,
                name=params.name or name,
                args=params.args,
                kwargs={**extras, **params.kwargs}
            )
        assert isinstance(params, collections.abc.Sequence)
        fn, *args = params
        if not unpack_arguments and len(args) == 1:
            args = args[0]
        (args, kwargs) = cls.normalize_params(args, extras, unpack_arguments)
        return cls(fn, name=name, args=args, kwargs=kwargs)

    @classmethod
    def build_jobs_from_params(cls, params, extras=None, unpack_arguments=True):
        params =_UniversalParallelParametersCollection(params)
        return [cls.normalize_job(name, param, extras, unpack_arguments) for name, param in params.iter_params()]

    @classmethod
    def build_for_callable_from_params(cls, fn, params, extras=None, unpack_arguments=True):
        params =_UniversalParallelParametersCollection(params)
        normalized = (
            (name, cls.normalize_params(param, extras, unpack_arguments))
            for name, param in params.iter_params()
        )
        return [
            cls(fn, name=name, args=args, kwargs=kwargs)
            for name, (args, kwargs) in normalized
        ]


class FailedTask:
    def __init__(self, job, exc):
        self.job = job
        self.exc = exc

    def __eq__(self, other):
        if type(self) != type(other):  # pragma: no cover
            return False
        return all([self.job == other.job, self.exc == other.exc])

    def __repr__(self):  # pragma: no cover
        return "FailedTask(job={}, exc={})".format(self.job, self.exc.__class__)


class BaseResult:  # pragma: no cover
    def new_result(self, name, result):
        raise NotImplementedError()

    @property
    def failures(self):
        raise NotImplementedError()

    @property
    def succeeded(self):
        raise NotImplementedError()

    @property
    def failed(self):
        raise NotImplementedError()

    def replace_failed(self, replacement):
        raise NotImplementedError()


class SequentialMapResult(BaseResult, list):
    def new_result(self, name, result):
        self.append(result)

    @property
    def failures(self):
        return bool([obj for obj in self if isinstance(obj, FailedTask)])

    @property
    def succeeded(self):
        return [obj for obj in self if not isinstance(obj, FailedTask)]

    @property
    def failed(self):
        return [obj for obj in self if isinstance(obj, FailedTask)]

    def replace_failed(self, replacement):
        return [replacement if isinstance(obj, FailedTask) else obj for obj in self]


class NamedMapResult(BaseResult, dict):
    def new_result(self, name, result):
        self[name] = result

    @property
    def failures(self):
        return bool([obj for obj in self.values() if isinstance(obj, FailedTask)])

    @property
    def succeeded(self):
        return {
            name: obj for name, obj in self.items() if not isinstance(obj, FailedTask)
        }

    @property
    def failed(self):
        return {name: obj for name, obj in self.items() if isinstance(obj, FailedTask)}

    def replace_failed(self, replacement):
        return {
            name: (replacement if isinstance(obj, FailedTask) else obj)
            for name, obj in self.items()
        }

import functools
import collections
import concurrent.futures as cf
from . import exceptions

__all__ = ["decorate", "arg", "future", "map", "async_map", "par", "async_par"]

__version__ = '0.0.2'
__author__ = 'Santiago Basulto <santiago.basulto@gmail.com>'

THREAD = "thread"
PROCESS = "process"


class FailedTask:
    def __init__(self, params=None, kwargs=None, ex=Exception):
        self.params = params
        self.kwargs = kwargs or {}
        self.ex = ex

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return (
            self.params == other.params
            and self.kwargs == other.kwargs
            and self.ex == other.ex
        )

    def __repr__(self):
        return "FailedTask(params={}, kwargs={}, ex={})".format(
            self.params, self.kwargs, self.ex.__class__)


class ParallelJob:
    def __init__(self, name, fn, *args, **kwargs):
        self.name = name
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.fn == other.fn
            and self.args == other.args
            and self.kwargs == other.kwargs
        )

    @classmethod
    def normalize(cls, name, fn, params, extras=None, unpack_arguments=True):
        kwargs = (extras or {}).copy()
        if isinstance(params, cls):
            args = params.args
            kwargs.update(params.kwargs)
            return cls(params.name or name, params.fn or fn, *args, **kwargs)
        if isinstance(params, tuple) or isinstance(params, list):
            if not unpack_arguments:
                return cls(name, fn, params, **kwargs)
            args = []
            for param in params:
                if type(param) == dict:
                    kwargs.update(param)
                else:
                    args.append(param)
            return cls(name, fn, *args, **kwargs)
        if isinstance(params, dict):
            return cls(name, fn, **{**params, **kwargs})
        return cls(name, fn, params)


class BaseResult:
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


class BaseParallelExecutor:
    def __init__(self, timeout=None, max_workers=None):
        self.jobs = None
        self.result_class = None

        self.timeout = timeout
        self.max_workers = max_workers
        ExecutorClass = self._get_executor_class()
        self._executor = ExecutorClass(max_workers=max_workers)

    def _get_executor_class(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.shutdown(wait=True)
        return False

    def shutdown(self, wait=True):
        self._executor.shutdown(wait)

    def results(self, timeout=None, silent=False):
        assert self.jobs is not None
        assert self.result_class is not None
        if hasattr(self, "_cached_results"):
            return getattr(self, "_cached_results")

        results = self.result_class()
        for future, function_details in self.jobs:
            name, *function_args = function_details
            try:
                try:
                    results.new_result(name, future.result(timeout or self.timeout))
                except cf.TimeoutError as e:
                    raise exceptions.TimeoutException()
            except Exception as e:
                if not silent:
                    raise e
                args, kwargs = function_args
                results.new_result(name, FailedTask(args or None, kwargs, e))

        self._cached_results = results
        return results

    def submit_jobs(self, jobs, result_class):
        assert self.jobs is None
        assert self.result_class is None
        futures = [
            (
                self._executor.submit(job.fn, *job.args, **job.kwargs),
                (job.name, job.args, job.kwargs),
            )
            for job in jobs
        ]

        self.jobs = futures
        self.result_class = result_class


class ThreadExecutor(BaseParallelExecutor):
    def _get_executor_class(self):
        return cf.ThreadPoolExecutor


class ProcessExecutor(BaseParallelExecutor):
    def _get_executor_class(self):
        # return cf.ProcessPoolExecutor
        return cf.ThreadPoolExecutor


class ParallelProxy:
    def __init__(self, ExecutorClass, timeout=None, max_workers=None):
        self.ExecutorClass = ExecutorClass
        self.timeout = timeout
        self.max_workers = max_workers

    def _normalize_options(self, timeout=None, max_workers=None):
        return {
            "timeout": timeout or self.timeout,
            "max_workers": max_workers or self.max_workers,
        }

    def future(self, fn, *args, **kwargs):
        return ParallelJob(None, fn, *args, **kwargs)

    def _get_result_class(self, params):
        if isinstance(params, dict):
            return NamedMapResult
        return SequentialMapResult

    def _normalize_parameters(self, params):
        "Split parameters to (Name, Parameter)"
        if isinstance(params, dict):
            return params.items()
        return ((None, param) for param in params)

    def _split_function_parameters(self, named_params, extras):
        for name, param in named_params:
            if callable(param):
                yield ParallelJob(name, param)
            elif not isinstance(param, ParallelJob) and callable(param[0]):
                yield ParallelJob.normalize(name, param[0], param[1:], extras)
            else:
                yield ParallelJob.normalize(name, None, param, extras)

    def _build_jobs_for_common_function(
        self, fn, named_params, extras, unpack_arguments
    ):
        jobs = (
            ParallelJob.normalize(name, fn, param, extras, unpack_arguments)
            for name, param in named_params
        )
        return jobs

    def par(self, params, max_workers=None, timeout=None, extras=None, silent=False):
        options = self._normalize_options(timeout, max_workers)
        timeout, max_workers = options["timeout"], options["max_workers"]

        result_class = self._get_result_class(params)
        named_params = self._normalize_parameters(params)

        jobs = self._split_function_parameters(named_params, extras)

        with self.ExecutorClass(max_workers=max_workers) as ex:
            ex.submit_jobs(jobs, result_class)
            return ex.results(timeout=timeout, silent=silent)

    def map(
        self,
        fn,
        params,
        max_workers=None,
        timeout=None,
        extras=None,
        silent=False,
        unpack_arguments=True,
    ):
        options = self._normalize_options(timeout, max_workers)
        timeout, max_workers = options["timeout"], options["max_workers"]

        result_class = self._get_result_class(params)
        named_params = self._normalize_parameters(params)
        jobs = self._build_jobs_for_common_function(
            fn, named_params, extras, unpack_arguments
        )

        with self.ExecutorClass(timeout=timeout, max_workers=max_workers) as ex:
            ex.submit_jobs(jobs, result_class)
            return ex.results(timeout=timeout, silent=silent)

    def async_map(
        self,
        fn,
        params,
        max_workers=None,
        extras=None,
        silent=False,
        unpack_arguments=True,
    ):
        options = self._normalize_options(max_workers=max_workers)
        timeout, max_workers = options["timeout"], options["max_workers"]

        ex = self.ExecutorClass(timeout=timeout, max_workers=max_workers)

        result_class = self._get_result_class(params)
        named_params = self._normalize_parameters(params)
        jobs = self._build_jobs_for_common_function(
            fn, named_params, extras, unpack_arguments
        )

        ex.submit_jobs(jobs, result_class)
        return ex

    def async_par(self, params, max_workers=None, extras=None):
        options = self._normalize_options(max_workers=max_workers)
        timeout, max_workers = options["timeout"], options["max_workers"]

        result_class = self._get_result_class(params)
        named_params = self._normalize_parameters(params)

        jobs = self._split_function_parameters(named_params, extras)

        ex = self.ExecutorClass(timeout=timeout, max_workers=max_workers)
        ex.submit_jobs(jobs, result_class)
        return ex


class ParallelCallable:
    def __init__(self, fn, ex=THREAD, timeout=None, max_workers=None):
        assert ex in {THREAD, PROCESS}

        self.fn = fn
        self.ex = ex
        self.timeout = timeout
        self.max_workers = max_workers
        self.thread = ParallelProxy(
            ThreadExecutor, timeout=timeout, max_workers=max_workers
        )
        self.process = ParallelProxy(
            ProcessExecutor, timeout=timeout, max_workers=max_workers
        )

        default_executor = self.thread if ex == THREAD else self.process
        self.map = functools.partial(default_executor.map, fn)
        self.async_map = functools.partial(default_executor.async_map, fn)

        self.future = functools.partial(default_executor.future, fn)

        functools.update_wrapper(self, fn)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)


def decorate(*args, **kwargs):
    if len(args) > 0 and callable(args[0]):
        # Decorating function
        return ParallelCallable(args[0])
    else:
        def wrapper(fn):
            return ParallelCallable(fn, *args, **kwargs)

        return wrapper


# == Public Interface ==
# (Shortcuts exposed by library)
arg = lambda *args, **kwargs: ParallelJob(None, None, *args, **kwargs)
arg.__doc__ = "TODO"

future = lambda fn, *args, **kwargs: ParallelJob(None, fn, *args, **kwargs)
future.__doc__ = "TODO"

thread = ParallelProxy(ThreadExecutor)
process = ParallelProxy(ProcessExecutor)


def map(
    fn,
    params,
    max_workers=None,
    timeout=None,
    extras=None,
    silent=False,
    unpack_arguments=True,
):
    # TODO: Accept custom executor
    return ParallelProxy(ThreadExecutor).map(
        fn,
        params,
        max_workers=max_workers,
        timeout=timeout,
        extras=extras,
        silent=silent,
        unpack_arguments=unpack_arguments,
    )


def async_map(fn, params, max_workers=None, extras=None, unpack_arguments=True):
    # TODO: Accept custom executor
    return ParallelProxy(ThreadExecutor).async_map(
        fn,
        params,
        max_workers=max_workers,
        extras=extras,
        unpack_arguments=unpack_arguments,
    )


def par(params, max_workers=None, timeout=None, extras=None, silent=False):
    # TODO: Accept custom executor
    return ParallelProxy(ThreadExecutor).par(
        params, max_workers=max_workers, timeout=timeout, extras=extras, silent=silent
    )

def async_par(params, max_workers=None, extras=None):
    # TODO: Accept custom executor
    return ParallelProxy(ThreadExecutor).async_par(params, max_workers=max_workers, extras=extras)


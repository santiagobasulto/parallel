import os
import enum

import functools
import itertools
import collections
import concurrent.futures as cf

from . import errors
from . import utils
from . import exceptions
from .models import (
    ParallelJob,
    ParallelArg,
    ParallelStatus,
    FailedTask,
    SequentialMapResult,
    NamedMapResult,
)


# __all__ = ["decorate", "arg", "future", "map", "async_map", "par", "async_par"]
__all__ = ["map", "async_map", "par", "async_par"]

__version__ = "0.9.1"
__author__ = "Santiago Basulto <santiago.basulto@gmail.com>"


class ExecutorStrategy(enum.Enum):
    THREAD_EXECUTOR = "thread"
    PROCESS_EXECUTOR = "process"


THREAD_EXECUTOR = ExecutorStrategy.THREAD_EXECUTOR
PROCESS_EXECUTOR = ExecutorStrategy.PROCESS_EXECUTOR


class BaseParallelExecutor:
    def __init__(
        self,
        jobs,
        max_workers=None,
        timeout=None,
        silent=False,
        ResultClass=SequentialMapResult,
    ):
        self.jobs = jobs
        self.max_workers = max_workers
        self.timeout = timeout
        self.silent = silent
        self.ResultClass = ResultClass
        self.__status = ParallelStatus.NOT_STARTED
        self.__executor = None
        self.__results = None

    def _get_executor_class(self):  # pragma: no cover
        raise NotImplementedError()

    @property
    def status(self):
        return self.__status

    def start(self):
        if self.__status == ParallelStatus.STARTED:
            raise exceptions.ParallelStatusException(errors.STATUS_EXECUTOR_RUNNING)
        self.__status = ParallelStatus.STARTED

        ExecutorClass = self._get_executor_class()
        self.__executor = ExecutorClass(max_workers=self.max_workers)
        for job in self.jobs:
            future = self.__executor.submit(job.fn, *job.args, **(job.kwargs))
            # TODO: Check status for ParallelJob
            job.future = future

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.shutdown()

    def results(self, timeout=None):
        if self.__results:
            return self.__results

        if self.__status == ParallelStatus.NOT_STARTED:
            raise exceptions.ParallelStatusException(errors.STATUS_EXECUTOR_NOT_STARTED)

        ResultClass = self.ResultClass
        self.__results = ResultClass()

        for job in self.jobs:
            try:
                result = job.future.result(timeout=(timeout or self.timeout))
            except cf.TimeoutError as e:
                raise exceptions.TimeoutException() from e
            except Exception as exc:
                if not self.silent:
                    self.__status = ParallelStatus.FAILED
                    raise exc
                self.__results.new_result(job.name, FailedTask(job, exc))
            else:
                self.__results.new_result(job.name, result)

        self.__status = ParallelStatus.DONE
        return self.__results

    def shutdown(self):
        self.__executor.shutdown()


class ThreadExecutor(BaseParallelExecutor):
    def _get_executor_class(self):
        return cf.ThreadPoolExecutor


class ProcessExecutor(BaseParallelExecutor):
    def _get_executor_class(self):
        return cf.ProcessPoolExecutor


EXECUTOR_MAPPING = {
    ExecutorStrategy.THREAD_EXECUTOR: ThreadExecutor,
    ExecutorStrategy.PROCESS_EXECUTOR: ProcessExecutor,
}


class ParallelHelper:
    def __init__(self, executor=ExecutorStrategy.THREAD_EXECUTOR):
        if isinstance(executor, ExecutorStrategy):
            executor = EXECUTOR_MAPPING[executor]
        self.ExecutorClass = executor

    def get_result_class(self, params):
        # return SequentialMapResult
        if isinstance(params, collections.abc.Sequence):
            return SequentialMapResult
        return NamedMapResult

    def map(
        self,
        fn,
        params,
        extras=None,
        unpack_arguments=True,
        max_workers=None,
        timeout=None,
        silent=False,
    ):
        jobs = ParallelJob.build_for_callable_from_params(
            fn, params, extras=extras, unpack_arguments=unpack_arguments
        )
        ResultClass = self.get_result_class(params)
        with self.ExecutorClass(
            jobs,
            max_workers=max_workers,
            timeout=timeout,
            silent=silent,
            ResultClass=ResultClass,
        ) as ex:
            return ex.results()

    def async_map(
        self,
        fn,
        params,
        extras=None,
        unpack_arguments=True,
        max_workers=None,
        timeout=None,
        silent=False,
    ):
        jobs = ParallelJob.build_for_callable_from_params(
            fn, params, extras=extras, unpack_arguments=unpack_arguments
        )
        ResultClass = self.get_result_class(params)
        ex = self.ExecutorClass(
            jobs,
            max_workers=max_workers,
            timeout=timeout,
            silent=silent,
            ResultClass=ResultClass,
        )
        return ex

    def par(
        self,
        params,
        extras=None,
        unpack_arguments=True,
        max_workers=None,
        timeout=None,
        silent=False,
    ):
        jobs = ParallelJob.build_jobs_from_params(
            params, extras=extras, unpack_arguments=unpack_arguments
        )
        ResultClass = self.get_result_class(params)
        with self.ExecutorClass(
            jobs,
            max_workers=max_workers,
            timeout=timeout,
            silent=silent,
            ResultClass=ResultClass,
        ) as ex:
            return ex.results()

    def split(
        self,
        collection,
        fn,
        executor=ExecutorStrategy.THREAD_EXECUTOR,
        workers=None,
        timeout=None,
        extras=None,
    ):
        workers = workers or min(32, (os.cpu_count() or 1) + 4)

        chunks = utils.split_collection(collection, workers)
        jobs = [
            ParallelJob(fn, None, [chunk], (extras or {}).copy())
            for chunk in chunks
        ]

        with self.ExecutorClass(
            jobs,
            max_workers=workers,
            timeout=timeout,
            ResultClass=SequentialMapResult,
        ) as ex:
            results = ex.results()
            return list(itertools.chain.from_iterable(results))
            #[item for sublist in l for item in sublist]


def map(
    fn,
    params,
    executor=ExecutorStrategy.THREAD_EXECUTOR,
    max_workers=None,
    timeout=None,
    extras=None,
    silent=False,
    unpack_arguments=True,
):
    return ParallelHelper(executor).map(
        fn,
        params,
        extras=extras,
        unpack_arguments=unpack_arguments,
        max_workers=max_workers,
        timeout=timeout,
        silent=silent,
    )


def async_map(
    fn,
    params,
    executor=ExecutorStrategy.THREAD_EXECUTOR,
    max_workers=None,
    timeout=None,
    extras=None,
    silent=False,
    unpack_arguments=True,
):
    return ParallelHelper(executor).async_map(
        fn,
        params,
        extras=extras,
        unpack_arguments=unpack_arguments,
        max_workers=max_workers,
        timeout=timeout,
        silent=silent,
    )


def par(
    params,
    executor=ExecutorStrategy.THREAD_EXECUTOR,
    max_workers=None,
    timeout=None,
    extras=None,
    silent=False,
    unpack_arguments=True,
):
    return ParallelHelper(executor).par(
        params,
        extras=extras,
        unpack_arguments=unpack_arguments,
        max_workers=max_workers,
        timeout=timeout,
        silent=silent,
    )


def split(
    collection,
    fn,
    executor=ExecutorStrategy.THREAD_EXECUTOR,
    workers=None,
    timeout=None,
    extras=None,
):
    return ParallelHelper(executor).split(
        collection, fn, extras=extras, workers=workers, timeout=timeout
    )


class ParallelCallable:
    def __init__(
        self, fn, executor, timeout, max_workers,
    ):

        self.fn = fn
        self.executor = executor
        self.timeout = timeout
        self.max_workers = max_workers

    def map(
        self,
        params,
        executor=None,
        max_workers=None,
        timeout=None,
        extras=None,
        silent=False,
        unpack_arguments=True,
    ):
        executor = executor or self.executor
        return ParallelHelper(executor).map(
            self.fn,
            params,
            extras=extras,
            unpack_arguments=unpack_arguments,
            max_workers=(max_workers or self.max_workers),
            timeout=(timeout or self.timeout),
            silent=silent,
        )

    def async_map(
        self,
        params,
        executor=None,
        max_workers=None,
        timeout=None,
        extras=None,
        silent=False,
        unpack_arguments=True,
    ):
        executor = executor or self.executor
        return ParallelHelper(executor).async_map(
            self.fn,
            params,
            extras=extras,
            unpack_arguments=unpack_arguments,
            max_workers=(max_workers or self.max_workers),
            timeout=(timeout or self.timeout),
            silent=silent,
        )

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)


class ParallelDecorator:
    def __init__(
        self,
        fn,
        executor=ExecutorStrategy.THREAD_EXECUTOR,
        timeout=None,
        max_workers=None,
    ):

        self.fn = fn
        self.thread = ParallelCallable(
            fn,
            ExecutorStrategy.THREAD_EXECUTOR,
            timeout=timeout,
            max_workers=max_workers,
        )
        self.process = ParallelCallable(
            fn,
            ExecutorStrategy.PROCESS_EXECUTOR,
            timeout=timeout,
            max_workers=max_workers,
        )
        if executor == ExecutorStrategy.THREAD_EXECUTOR:
            self.default_executor = self.thread
        else:
            self.default_executor = self.process

    map = lambda self, *args, **kwargs: self.default_executor.map(*args, **kwargs)

    async_map = lambda self, *args, **kwargs: self.default_executor.async_map(
        *args, **kwargs
    )

    def future(self, *args, **kwargs):
        return job(self.fn, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)


def decorate(*args, **kwargs):
    if len(args) == 1 and callable(args[0]):
        # Invoked without parameters
        obj = ParallelDecorator(args[0])
        return obj
    else:

        def wrapper(fn):
            obj = ParallelDecorator(fn, *args, **kwargs)
            return obj

        return wrapper


thread = ParallelHelper(ThreadExecutor)
process = ParallelHelper(ProcessExecutor)

arg = lambda *args, **kwargs: ParallelArg(*args, **kwargs)
arg.__doc__ = "TODO"


def job(*args, **kwargs):
    "TODO"
    fn, *args = args
    return ParallelJob(fn, None, args, kwargs)


NOT_STARTED = ParallelStatus.NOT_STARTED
STARTED = ParallelStatus.STARTED
DONE = ParallelStatus.DONE
FAILED = ParallelStatus.FAILED

import enum

import functools
import collections
import concurrent.futures as cf

from . import errors
from . import exceptions
from .models import (
    ParallelJob, ParallelArg,
    ParallelStatus, FailedTask,
    SequentialMapResult, NamedMapResult
)


#__all__ = ["decorate", "arg", "future", "map", "async_map", "par", "async_par"]
__all__ = ["map", "async_map", "par", "async_par"]

__version__ = '0.0.2'
__author__ = 'Santiago Basulto <santiago.basulto@gmail.com>'


class ExecutorStrategy(enum.Enum):
    THREAD_EXECUTOR = "thread"
    PROCESS_EXECUTOR = "process"

THREAD_EXECUTOR = ExecutorStrategy.THREAD_EXECUTOR
PROCESS_EXECUTOR = ExecutorStrategy.PROCESS_EXECUTOR


class BaseParallelExecutor:
    def __init__(self, jobs, max_workers=None, timeout=None, silent=False, ResultClass=SequentialMapResult):
        self.jobs = jobs
        self.max_workers = max_workers
        self.timeout = timeout
        self.silent = silent
        self.ResultClass = ResultClass
        self.__status = ParallelStatus.NOT_STARTED
        self.__executor = None
        self.__results = None

    def _get_executor_class(self):
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

        # ResultClass = self.get_result_class()
        self.__results = self.ResultClass()
        if self.__status == ParallelStatus.NOT_STARTED:
            raise exceptions.ParallelStatusException(errors.STATUS_EXECUTOR_NOT_STARTED)
        assert self.__status in {ParallelStatus.STARTED, ParallelStatus.DONE}

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
        assert self.__status in {ParallelStatus.DONE, ParallelStatus.FAILED}
        self.__executor.shutdown()


class ThreadExecutor(BaseParallelExecutor):
    def _get_executor_class(self):
        return cf.ThreadPoolExecutor


class ProcessExecutor(BaseParallelExecutor):
    def _get_executor_class(self):
        return cf.ProcessPoolExecutor


EXECUTOR_MAPPING = {
    ExecutorStrategy.THREAD_EXECUTOR: ThreadExecutor,
    ExecutorStrategy.PROCESS_EXECUTOR: ProcessExecutor
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
        jobs = ParallelJob.build_from_params(
            fn, params, extras=extras, unpack_arguments=unpack_arguments)
        ResultClass = self.get_result_class(params)
        with self.ExecutorClass(jobs, max_workers=max_workers, timeout=timeout, silent=silent, ResultClass=ResultClass) as ex:
            return ex.results()

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
    return ParallelHelper(executor).map(fn, params, extras=extras,
                                 unpack_arguments=unpack_arguments, max_workers=max_workers,
                                 timeout=timeout, silent=silent)

thread = ParallelHelper(ThreadExecutor)
process = ParallelHelper(ProcessExecutor)

arg = lambda *args, **kwargs: ParallelArg(*args, **kwargs)
arg.__doc__ = "TODO"

NOT_STARTED = ParallelStatus.NOT_STARTED
STARTED = ParallelStatus.STARTED
DONE = ParallelStatus.DONE
FAILED = ParallelStatus.FAILED
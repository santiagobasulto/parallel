import functools
import collections
import concurrent.futures as cf

from abc import ABCMeta, abstractmethod
from typing import (
    Type, Optional, Union, Any, Callable, Mapping, Literal, Iterable, TypeVar,
    List
)

from . import errors
from . import exceptions


#__all__ = ["decorate", "arg", "future", "map", "async_map", "par", "async_par"]

__version__ = '0.0.2'
__author__ = 'Santiago Basulto <santiago.basulto@gmail.com>'

ExecutorType = TypeVar('ExecutorType', bound=cf.Executor)


from enum import Enum

class ExecutorStrategy(Enum):
    THREAD_EXECUTOR = "thread"
    PROCESS_EXECUTOR = "process"

THREAD_EXECUTOR = ExecutorStrategy.THREAD_EXECUTOR
PROCESS_EXECUTOR = ExecutorStrategy.PROCESS_EXECUTOR


class BaseParallelExecutor:
    def __init__(self, fn: Callable) -> None:
        self.fn = fn

    @abstractmethod
    def _get_executor_class(self) -> Type[cf.Executor]: pass

    def map(
        self,
        params: Iterable,
        max_workers: Optional[int]=None,
        timeout: Optional[int]=None,
        extras: Optional[Mapping]=None,
        silent: bool=False,
        unpack_arguments: bool=True,
    ) -> Any:
        pass
        # if hasattr(self, '__decorated') and self.ExecutorClass == ProcessExecutor:
        #     raise NotImplementedError(errors.DECORATED_PROCESS_FUNCTION)
        # options = self._normalize_options(timeout, max_workers)
        # timeout, max_workers = options["timeout"], options["max_workers"]

        # result_class = self._get_result_class(params)
        # named_params = self._normalize_parameters(params)
        # jobs = self._build_jobs_for_common_function(
        #     fn, named_params, extras, unpack_arguments
        # )
        ExecutorClass: Type[cf.Executor] = self._get_executor_class()
        with ExecutorClass as ex:
            futures: List[cf.Future] = [
                ex.submit(self.fn, param)
                for param in params
            ]
            return [future.result() for future in futures]

        # with self.ExecutorClass(timeout=timeout, max_workers=max_workers) as ex:
        #     ex.submit_jobs(jobs, result_class)
        #     return ex.results(timeout=timeout, silent=silent)


class ThreadExecutor(BaseParallelExecutor):
    def _get_executor_class(self) -> Type[cf.ThreadPoolExecutor]:
        return cf.ThreadPoolExecutor


class ProcessExecutor(BaseParallelExecutor):
    def _get_executor_class(self) -> Type[cf.ProcessPoolExecutor]:
        return cf.ProcessPoolExecutor

EXECUTOR_MAPPING: Mapping[ExecutorStrategy, Type[BaseParallelExecutor]] = {
    ExecutorStrategy.THREAD_EXECUTOR: ThreadExecutor,
    ExecutorStrategy.PROCESS_EXECUTOR: ProcessExecutor
}

def map(
    fn: Callable,
    params: Any,
    # executor: Optional[Union[Type[BaseParallelExecutor], str]] = ThreadExecutor,
    executor: Literal[ExecutorStrategy.THREAD_EXECUTOR]=ExecutorStrategy.THREAD_EXECUTOR,
    max_workers: Optional[int] = None,
    timeout: int = None,
    extras: dict = None,
    silent: bool = False,
    unpack_arguments: bool = True,
):
    # TODO: Accept custom executor
    # if isinstance(executor, str):
    # executor_class: BaseParallelExecutor = EXECUTOR_MAPPING[executor]

    # return executor_class(fn).map(
    #     params,
    #     max_workers=max_workers,
    #     timeout=timeout,
    #     extras=extras,
    #     silent=silent,
    #     unpack_arguments=unpack_arguments,
    # )
    pass

import time


def sleep_return_single_param(sleep: float) -> str:
    time.sleep(sleep)
    return str(sleep)

print(map(sleep_return_single_param, [.2, .3, .1]))
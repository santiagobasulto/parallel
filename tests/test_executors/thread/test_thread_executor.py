import pytest
from unittest.mock import MagicMock

import parallel
from parallel import ThreadExecutor
from parallel import exceptions
from parallel.models import ParallelJob, FailedTask


class TestingException(Exception):
    def __eq__(self, other):
        return type(self) == type(other) and self.args == other.args


def test_executor_single_job_only_args():
    mocked_fn = MagicMock(return_value=None)
    jobs = [ParallelJob(mocked_fn, args=(2, 2))]

    ex = ThreadExecutor(jobs)
    ex.start()
    assert ex.results() == [None]
    ex.shutdown()
    mocked_fn.assert_called_once_with(2, 2)


def test_executor_single_job_args_kwargs():
    mocked_fn = MagicMock(return_value=None)
    jobs = [ParallelJob(mocked_fn, args=(2, 2), kwargs={'a': 'b'})]

    ex = ThreadExecutor(jobs)
    ex.start()
    assert ex.results() == [None]
    ex.shutdown()
    mocked_fn.assert_called_once_with(2, 2, a='b')


def test_executor_multiple_jobs_same_function():
    mocked_fn = MagicMock(return_value=None)
    jobs = [
        ParallelJob(mocked_fn, args=(2, 2)),
        ParallelJob(mocked_fn, args=('a', 'b'), kwargs={'x': 'y'}),
    ]

    ex = ThreadExecutor(jobs)
    ex.start()
    assert ex.results() == [None, None]
    ex.shutdown()
    mocked_fn.assert_any_call(2, 2)
    mocked_fn.assert_any_call('a', 'b', x='y')


def test_executor_multiple_jobs_diff_function():
    mocked_fn1 = MagicMock(return_value=None)
    mocked_fn2 = MagicMock(return_value=None)
    jobs = [
        ParallelJob(mocked_fn1, args=(2, 2)),
        ParallelJob(mocked_fn2, args=('a', 'b'), kwargs={'x': 'y'}),
    ]

    ex = ThreadExecutor(jobs)
    ex.start()
    assert ex.results() == [None, None]
    ex.shutdown()
    mocked_fn1.assert_called_once_with(2, 2)
    mocked_fn2.assert_called_once_with('a', 'b', x='y')


# Exceptions & Error Handling
def test_executor_propagates_exceptions_raised():
    mocked_fn = MagicMock(side_effect=ValueError('Testing'))
    jobs = [
        ParallelJob(mocked_fn, args=(2, 2)),
    ]

    ex = ThreadExecutor(jobs)
    ex.start()
    with pytest.raises(ValueError):
        ex.results()
    ex.shutdown()
    assert ex.status is parallel.FAILED


def test_silent_exception_caught():
    mocked_fn1 = MagicMock(side_effect=TestingException('Testing'))
    mocked_fn2 = MagicMock(return_value=None)
    jobs = [
        ParallelJob(mocked_fn1, args=(2, 2)),
        ParallelJob(mocked_fn2, args=('a', 'b'), kwargs={'x': 'y'}),
    ]

    ex = ThreadExecutor(jobs, silent=True)
    ex.start()

    assert ex.results() == [FailedTask(jobs[0], TestingException('Testing')), None]
    ex.shutdown()

# Executor Sate
def test_executor_not_started_results_raises_exception():
    mocked_fn = MagicMock(return_value=None)
    job = ParallelJob(mocked_fn, args=(2, 2))

    ex = ThreadExecutor([job])
    with pytest.raises(exceptions.ParallelStatusException):
        ex.results()


def test_executor_started_raises_exception():
    mocked_fn = MagicMock(return_value=None)
    job = ParallelJob(mocked_fn, args=(2, 2))

    ex = ThreadExecutor([job])
    ex.start()
    with pytest.raises(exceptions.ParallelStatusException):
        ex.start()


# Pending
# * Errors in state

# Nice to have
# * Retries
# *
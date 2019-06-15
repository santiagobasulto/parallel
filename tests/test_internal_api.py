import time
import pytest

import parallel
from parallel.exceptions import TimeoutException


def sleep_return(n):
    time.sleep(n)
    return str(n)


def sleep_seq(a_seq):
    for n in a_seq:
        time.sleep(n)
    return " - ".join([str(n) for n in a_seq])


def test_thread_executor_same_fn_seq():
    ex = parallel.ThreadExecutor()
    ex.submit_jobs(
        [
            parallel.ParallelJob(None, sleep_return, 0.3),
            parallel.ParallelJob(None, sleep_return, 0.1),
            parallel.ParallelJob(None, sleep_return, 0.2),
        ],
        parallel.SequentialMapResult,
    )

    assert ex.results() == ["0.3", "0.1", "0.2"]


def test_thread_executor_same_fn_dict():
    ex = parallel.ThreadExecutor()
    ex.submit_jobs(
        [
            parallel.ParallelJob("r1", sleep_return, 0.3),
            parallel.ParallelJob("r2", sleep_return, 0.1),
            parallel.ParallelJob("r3", sleep_return, 0.2),
        ],
        parallel.NamedMapResult,
    )

    assert ex.results() == {"r1": "0.3", "r2": "0.1", "r3": "0.2"}


def test_thread_executor_diff_fn():
    ex = parallel.ThreadExecutor()
    ex.submit_jobs(
        [
            parallel.ParallelJob(None, sleep_return, 0.3),
            parallel.ParallelJob(None, sleep_seq, [0.1, 0.2]),
        ],
        parallel.SequentialMapResult,
    )

    assert ex.results() == ["0.3", "0.1 - 0.2"]


def test_thread_executor_diff_fn_dict():
    ex = parallel.ThreadExecutor()
    ex.submit_jobs(
        [
            parallel.ParallelJob("r1", sleep_return, 0.3),
            parallel.ParallelJob("r2", sleep_seq, [0.1, 0.2]),
        ],
        parallel.NamedMapResult,
    )

    assert ex.results() == {"r1": "0.3", "r2": "0.1 - 0.2"}


def test_normalize():
    job = parallel.ParallelJob.normalize(
        'j1', sleep_return,
        {'p3': 'R', 'sleep': 0.3},
        extras={'p1': 'X', 'p2': 'Y'})
    assert job.args == tuple()
    assert job.kwargs == {
        'p1': 'X',
        'p2': 'Y',
        'p3': 'R',
        'sleep': 0.3
    }
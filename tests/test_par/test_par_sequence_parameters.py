import time
import pytest

import parallel
from parallel.models import ParallelJob

from ..base import *


def test_par_sequence_ordered_params():
    results = parallel.par([
        (sleep_return_multi_param, .1, 'a'),
        parallel.job(sleep_return_single_param, .2),
    ])
    assert results == ['a', '0.2']


def test_par_dict_ordered_params():
    results = parallel.par({
        'r1': (sleep_return_multi_param, .1, 'a'),
        'r2': parallel.job(sleep_return_single_param, .2),
    })
    assert results == {
        'r1': 'a',
        'r2': '0.2'
    }


def test_par_sequence_named_params():
    results = parallel.par([
        (sleep_return_optional_param, .1, 'a', {'uppercase': True}),
        parallel.job(sleep_return_optional_param, .2, result='b', uppercase=True),
    ])
    assert results == ['A', 'B']


def test_par_dict_named_params():
    results = parallel.par({
        'r1': (sleep_return_optional_param, .1, 'a', {'uppercase': True}),
        'r2': parallel.job(sleep_return_optional_param, .2, result='b', uppercase=True),
    })
    assert results == {
        'r1': 'A',
        'r2': 'B'
    }

def test_par_sequence_extras():
    results = parallel.par([
        (sleep_return_optional_param, .1, 'a', {'uppercase': False}),
        parallel.job(sleep_return_optional_param, .2, result='b', uppercase=False),
        (sleep_return_optional_param, .1, 'c',),
        parallel.job(sleep_return_optional_param, .2, result='d'),
    ], extras={
        'uppercase': True
    })
    assert results == ['a', 'b', 'C', 'D']


def test_par_sequence_unpack_arguments():
    results = parallel.par([
        (sleep_return_tuple, (.2, 'a')),
        (sleep_return_tuple, (.1, 'b')),
    ], unpack_arguments=False)

    assert results == ['a', 'b']


def test_par_dict_unpack_arguments():
    results = parallel.par({
        'r1': (sleep_return_tuple, (.2, 'a')),
        'r2': (sleep_return_tuple, (.1, 'b')),
    }, unpack_arguments=False)

    assert results == {
        'r1': 'a',
        'r2': 'b',
    }


def test_par_failure_propagated():
    with pytest.raises(TestingException):
        parallel.par([
            (sleep_return_multi_param, .2, 'a'),
            (sleep_return_multi_param, 'Will Fail', 'b')
        ])


def test_par_silent_errors_sequence():
    results = parallel.par([
        (sleep_return_multi_param, .2, 'a'),
        (sleep_return_multi_param, 'Will Fail', 'b'),
        (sleep_return_multi_param, .1, 'c'),
    ], silent=True)

    assert results.failures is True

    assert results == [
        'a',
        parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, args=('Will Fail', 'b')),
            exc=TestingException('Will Fail')
        ),
        'c'
    ]

    assert results.replace_failed(None) == ['a', None, 'c']


def test_par_silent_errors_dict():
    results = parallel.par({
        'r1': (sleep_return_multi_param, .2, 'a'),
        'r2': (sleep_return_multi_param, 'Will Fail', 'b'),
        'r3': (sleep_return_multi_param, .1, 'c'),
    }, silent=True)

    assert results.failures is True

    assert results == {
        'r1': 'a',
        'r2': parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, 'r2', args=('Will Fail', 'b')),
            exc=TestingException('Will Fail')
        ),
        'r3': 'c'
    }

    assert results.replace_failed(None) == {
        'r1': 'a',
        'r2': None,
        'r3': 'c'
    }

###########
# Timeout #
###########
def test_par_sequence_timeout():
    with pytest.raises(parallel.exceptions.TimeoutException):
        parallel.par([
            (sleep_return_single_param, 1)
        ], timeout=.1)


def test_par_dict_timeout():
    with pytest.raises(parallel.exceptions.TimeoutException):
        parallel.par({
            'r1': (sleep_return_single_param, 1)
        }, timeout=.1)


def test_par_sequence_max_workers():
    results = parallel.par([
        (sleep_return_single_param, .1),
        (sleep_return_single_param, .2),
        (sleep_return_single_param, .3),
    ], max_workers=1)

    assert results == ['0.1', '0.2', '0.3']
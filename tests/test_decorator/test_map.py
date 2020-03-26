import time
import pytest

import parallel
from parallel.models import ParallelJob

from ..base import *


def test_function_call():
    assert sleep_return_single_param_decorated(.2) == '0.2'


def test_map_basic_single_param():
    results = sleep_return_single_param_decorated.map([.2, .3, .1])
    assert results == ['0.2', '0.3', '0.1']

    results = sleep_return_single_param_decorated.thread.map([.2, .3, .1])
    assert results == ['0.2', '0.3', '0.1']

    results = sleep_return_single_param_decorated.map({
        'r1': .2,
        'r2': .3
    })
    assert results == {
        'r1': '0.2',
        'r2': '0.3'
    }

    results = sleep_return_single_param_decorated.thread.map({
        'r1': .2,
        'r2': .3
    })
    assert results == {
        'r1': '0.2',
        'r2': '0.3'
    }


def test_map_multiple_params():
    results = sleep_return_multi_param_decorated.map([
        (.2, 'a'), (.3, 'b'), (.1, 'c')
    ])
    assert results == ['a', 'b', 'c']

    results = sleep_return_multi_param_decorated.thread.map([
        (.2, 'a'), (.3, 'b'), (.1, 'c')
    ])
    assert results == ['a', 'b', 'c']

    results = sleep_return_multi_param_decorated.map({
        'r1': (.2, 'a'),
        'r2': (.3, 'b')
    })
    assert results == {
        'r1': 'a',
        'r2': 'b'
    }

    results = sleep_return_multi_param_decorated.thread.map({
        'r1': (.2, 'a'),
        'r2': (.3, 'b')
    })
    assert results == {
        'r1': 'a',
        'r2': 'b'
    }

def test_map_extra_params():
    results = sleep_return_optional_param_decorated.map([
        (.2, 'a'), (.3, {'result': 'b', 'uppercase': False}), (.1, 'c')
    ], extras={
        'uppercase': True
    })
    assert results == ['A', 'b', 'C']

    results = sleep_return_optional_param_decorated.map({
        'r1': (.2, 'a'),
        'r2': (.3, {'result': 'b', 'uppercase': False}),
        'r3': (.1, 'c'),
    }, extras={
        'uppercase': True
    })
    assert results == {
        'r1': 'A',
        'r2': 'b',
        'r3': 'C',
    }


def test_map_unpack_arguments_false():
    results = sleep_return_tuple_decorated.map([
        (.2, 'a'), (.3, 'b'), (.1, 'c')
    ], unpack_arguments=False)

    assert results == ['a', 'b', 'c']

    results = sleep_return_tuple_decorated.map({
        'r1': (.2, 'a'),
        'r2': (.3, 'b'),
        'r3': (.1, 'c')
    }, unpack_arguments=False)

    assert results == {
        'r1': 'a',
        'r2': 'b',
        'r3': 'c',
    }


def test_map_parallel_arg():
    results = sleep_return_optional_param_decorated.map([
        parallel.arg(.2, result='a'),
        parallel.arg(.3, result='b', uppercase=False),
        parallel.arg(.1, result='c')
    ], extras={
        'uppercase': True
    })
    assert results == ['A', 'b', 'C']

    results = sleep_return_optional_param_decorated.map({
        'r1': parallel.arg(.2, result='a'),
        'r2': parallel.arg(.3, result='b', uppercase=False),
        'r3': parallel.arg(.1, result='c')
    }, extras={
        'uppercase': True
    })
    assert results == {
        'r1': 'A',
        'r2': 'b',
        'r3': 'C',
    }


def test_map_exceptions_propagated():
    with pytest.raises(TestingException):
        sleep_return_multi_param_decorated.map([
            (.2, 'a'), ('Will Fail', 'b'), (.1, 'c')
        ])


def test_map_silent_mode():
    results = sleep_return_multi_param_decorated.map([
        (.2, 'a'), ('Will Fail', 'b'), (.1, 'c')
    ], silent=True)

    assert results.failures is True

    a, failed_task, c = results
    assert (a, c) == ('a', 'c')
    assert isinstance(failed_task, parallel.FailedTask)

    results = sleep_return_multi_param_decorated.map({
            'r1': (.2, 'a'),
            'r2': ('Will Fail', 'b'),
            'r3': (.1, 'c')
        }, silent=True)

    assert results.failures is True

    a, failed_task, c = [results[k] for k in ['r1', 'r2', 'r3']]
    assert (a, c) == ('a', 'c')
    assert isinstance(failed_task, parallel.FailedTask)
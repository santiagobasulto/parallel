import time
import pytest

import parallel
from parallel.exceptions import TimeoutException

from .base import (
    BaseSameFunctionParallelTestInvokationTestCase,
    BaseMultipleFunctionsParallelErrorHandlingTestCase,
    BaseMultipleFunctionParallelOtherOptionsTestCase,
    TestingException,
    ten_decorated, add_sleep_decorated, multiply_sleep_decorated, power_decorated, error_function, error_function_decorated)


class TestInvokation(BaseSameFunctionParallelTestInvokationTestCase):
    def test_basic_function_no_parameters(self):
        results = parallel.par([
            ten_decorated.future(),
            parallel.future(ten_decorated)
        ])
        assert results == [10, 10]

    def test_basic_function_single_parameter(self):
        results = parallel.par([
            power_decorated.future(2),
            power_decorated.future(3)
        ])
        assert results == ['4', '9']

    def test_basic_multiple_parameters(self):
        results = parallel.par([
            add_sleep_decorated.future(.05, .03),
            multiply_sleep_decorated.future(.05, 2)
        ])
        assert results == ['0.08', '0.1']

    def test_parameters_and_named_arguments(self):
        results = parallel.par([
            power_decorated.future(3, power=3),
            multiply_sleep_decorated.future(.05, 2)
        ])

        assert results == ['27', '0.1']

    def test_parameters_and_extra_argument(self):
        results = parallel.par([
            power_decorated.future(2),
            power_decorated.future(3),
            power_decorated.future(4),
        ], extras={
            'power': 3
        })
        assert results == ['8', '27', '64']

    def test_parameters_named_arguments_and_extras(self):
        results = parallel.par([
            power_decorated.future(x=2),
            power_decorated.future(3),
            power_decorated.future(x=4),
        ], extras={
            'power': 3
        })
        assert results == ['8', '27', '64']

    def test_parameters_named_arguments_replaces_extras(self):
        results = parallel.par([
            power_decorated.future(2),
            power_decorated.future(3, power=2),
            power_decorated.future(4, power=4),
        ], extras={
            'power': 3
        })
        assert results == ['8', '9', '256']

    def test_parallel_future_with_params_and_named_arguments(self):
        results = parallel.par([
            power_decorated.future(2),
            power_decorated.future(3, 3),
            power_decorated.future(4, power=4),
        ])
        assert results == ['4', '27', '256']

    def test_parallel_future_and_sequence_params(self):
        results = parallel.par([
            (power_decorated, 2),
            power_decorated.future(3, 3),
            (power_decorated, 4, {'power': 4}),
        ])
        assert results == ['4', '27', '256']

    def test_parallel_future_with_params_and_named_arguments_and_extras(self):
        results = parallel.par([
            power_decorated.future(2),
            power_decorated.future(3, power=3),
            power_decorated.future(4),
        ], extras={
            'power': 4
        })
        assert results == ['16', '27', '256']


class TestErrorHandling(BaseMultipleFunctionsParallelErrorHandlingTestCase):
    def test_error_handling_without_silent_propagates(self):
        with pytest.raises(TestingException):
            parallel.par([
                error_function_decorated.future("Test 1"),
                error_function_decorated.future("Test 2"),
            ])

    def test_error_handling_silent_with_params(self):
        results = parallel.par([
            add_sleep_decorated.future(.05, .03),
            error_function_decorated.future('Test 1'),
            multiply_sleep_decorated.future(.05, 2)
        ], silent=True)

        assert results.failures is True

        assert results == [
            '0.08',
            parallel.FailedTask(
                params=('Test 1', ),
                ex=TestingException('Error: Test 1')
            ),
            '0.1'
        ]

        assert results.succeeded == ['0.08', '0.1']
        assert results.failed == [parallel.FailedTask(
            params=('Test 1', ),
            ex=TestingException('Error: Test 1')
        )]
        assert results.replace_failed(None) == ['0.08', None, '0.1']


class TestOptions(BaseMultipleFunctionParallelOtherOptionsTestCase):

    def test_options_timeout_modifier(self):
        with pytest.raises(TimeoutException):
            parallel.par([
                add_sleep_decorated.future(.5, 1)
            ], timeout=1)

    def test_options_max_workers_modifier(self):
        results = parallel.par([
            (add_sleep_decorated, .05, .03),
            (multiply_sleep_decorated, .05, 2),
            (power_decorated, 3)
        ], max_workers=1)

        assert results == ['0.08', '0.1', '9']

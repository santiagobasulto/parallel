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
        params = [
            ten_decorated.future(),
            parallel.future(ten_decorated)
        ]
        with parallel.async_par(params) as ex:
            results = ex.results()
        assert results == [10, 10]

    def test_basic_function_single_parameter(self):
        params = [
            power_decorated.future(2),
            power_decorated.future(3)
        ]
        with parallel.async_par(params) as ex:
            results = ex.results()
        assert results == ['4', '9']

    def test_basic_multiple_parameters(self):
        params = [
            add_sleep_decorated.future(.05, .03),
            multiply_sleep_decorated.future(.05, 2)
        ]
        with parallel.async_par(params) as ex:
            results = ex.results()
        assert results == ['0.08', '0.1']

    def test_parameters_and_named_arguments(self):
        params = [
            power_decorated.future(3, power=3),
            multiply_sleep_decorated.future(.05, 2)
        ]
        with parallel.async_par(params) as ex:
            results = ex.results()

        assert results == ['27', '0.1']

    def test_parameters_and_extra_argument(self):
        params = [
            power_decorated.future(2),
            power_decorated.future(3),
            power_decorated.future(4),
        ]
        with parallel.async_par(params, extras={'power': 3}) as ex:
            results = ex.results()
        assert results == ['8', '27', '64']

    def test_parameters_named_arguments_and_extras(self):
        params = [
            power_decorated.future(x=2),
            power_decorated.future(3),
            power_decorated.future(x=4),
        ]
        with parallel.async_par(params, extras={'power': 3}) as ex:
            results = ex.results()
        assert results == ['8', '27', '64']

    def test_parameters_named_arguments_replaces_extras(self):
        params = [
            power_decorated.future(2),
            power_decorated.future(3, power=2),
            power_decorated.future(4, power=4),
        ]
        with parallel.async_par(params, extras={'power': 3}) as ex:
            results = ex.results()

        assert results == ['8', '9', '256']

    def test_parallel_future_with_params_and_named_arguments(self):
        params = [
            power_decorated.future(2),
            power_decorated.future(3, 3),
            power_decorated.future(4, power=4),
        ]
        with parallel.async_par(params) as ex:
            results = ex.results()
        assert results == ['4', '27', '256']

    def test_parallel_future_and_sequence_params(self):
        params = [
            (power_decorated, 2),
            power_decorated.future(3, 3),
            (power_decorated, 4, {'power': 4}),
        ]
        with parallel.async_par(params) as ex:
            results = ex.results()
        assert results == ['4', '27', '256']

    def test_parallel_future_with_params_and_named_arguments_and_extras(self):
        params = [
            power_decorated.future(2),
            power_decorated.future(3, power=3),
            power_decorated.future(4),
        ]
        with parallel.async_par(params, extras={'power': 4}) as ex:
            results = ex.results()
        assert results == ['16', '27', '256']


class TestErrorHandling(BaseMultipleFunctionsParallelErrorHandlingTestCase):
    def test_error_handling_without_silent_propagates(self):
        params = [
            error_function_decorated.future("Test 1"),
            error_function_decorated.future("Test 2"),
        ]
        with parallel.async_par(params) as ex:
            with pytest.raises(TestingException):
                ex.results()

    def test_error_handling_silent_with_params(self):
        params = [
            add_sleep_decorated.future(.05, .03),
            error_function_decorated.future('Test 1'),
            multiply_sleep_decorated.future(.05, 2)
        ]
        with parallel.async_par(params) as ex:
            results = ex.results(silent=True)

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
        params = [
            add_sleep_decorated.future(.5, 1)
        ]
        with parallel.async_par(params) as ex:
            with pytest.raises(TimeoutException):
                ex.results(timeout=1)

    def test_options_max_workers_modifier(self):
        params = [
            (add_sleep_decorated, .05, .03),
            (multiply_sleep_decorated, .05, 2),
            (power_decorated, 3)
        ]
        with parallel.async_par(params, max_workers=1) as ex:
            results = ex.results()

        assert results == ['0.08', '0.1', '9']

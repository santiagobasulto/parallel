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
        params = {
            'r1': ten_decorated.future(),
            'r2': parallel.future(ten_decorated)
        }
        with parallel.async_par(params) as ex:
            results = ex.results()
        assert results == {
            'r1': 10,
            'r2': 10
        }

    def test_basic_function_single_parameter(self):
        params = {
            'r1': power_decorated.future(2),
            'r2': power_decorated.future(3)
        }
        with parallel.async_par(params) as ex:
            results = ex.results()
        assert results == {
            'r1': '4',
            'r2': '9'
        }

    def test_basic_multiple_parameters(self):
        params = {
            'r1': add_sleep_decorated.future(.05, .03),
            'r2': multiply_sleep_decorated.future(.05, 2)
        }
        with parallel.async_par(params) as ex:
            results = ex.results()
        assert results == {
            'r1': '0.08',
            'r2': '0.1'
        }

    def test_parameters_and_named_arguments(self):
        params = {
            'r1': power_decorated.future(3, power=3),
            'r2': multiply_sleep_decorated.future(.05, 2)
        }
        with parallel.async_par(params) as ex:
            results = ex.results()

        assert results == {
            'r1': '27',
            'r2': '0.1'
        }

    def test_parameters_and_extra_argument(self):
        params = {
            'r1': power_decorated.future(2),
            'r2': power_decorated.future(3),
            'r3': power_decorated.future(4),
        }
        with parallel.async_par(params, extras={'power': 3}) as ex:
            results = ex.results()
        assert results == {
            'r1': '8',
            'r2': '27',
            'r3': '64'
        }

    def test_parameters_named_arguments_and_extras(self):
        params = {
            'r1': power_decorated.future(x=2),
            'r2': power_decorated.future(3),
            'r3': power_decorated.future(x=4),
        }
        with parallel.async_par(params, extras={'power': 3}) as ex:
            results = ex.results()
        assert results == {
            'r1': '8',
            'r2': '27',
            'r3': '64'
        }

    def test_parameters_named_arguments_replaces_extras(self):
        params = {
            'r1': power_decorated.future(2),
            'r2': power_decorated.future(3, power=2),
            'r3': power_decorated.future(4, power=4),
        }
        with parallel.async_par(params, extras={'power': 3}) as ex:
            results = ex.results()

        assert results == {
            'r1': '8',
            'r2': '9',
            'r3': '256'
        }

    def test_parallel_future_with_params_and_named_arguments(self):
        params = {
            'r1': power_decorated.future(2),
            'r2': power_decorated.future(3, 3),
            'r3': power_decorated.future(4, power=4),
        }
        with parallel.async_par(params) as ex:
            results = ex.results()
        assert results == {
            'r1': '4',
            'r2': '27',
            'r3': '256'
        }

    def test_parallel_future_and_sequence_params(self):
        params = {
            'r1': (power_decorated, 2),
            'r2': power_decorated.future(3, 3),
            'r3': (power_decorated, 4, {'power': 4}),
        }
        with parallel.async_par(params) as ex:
            results = ex.results()
        assert results == {
            'r1': '4',
            'r2': '27',
            'r3': '256'
        }

    def test_parallel_future_with_params_and_named_arguments_and_extras(self):
        params = {
            'r1': power_decorated.future(2),
            'r2': power_decorated.future(3, power=3),
            'r3': power_decorated.future(4),
        }
        with parallel.async_par(params, extras={'power': 4}) as ex:
            results = ex.results()
        assert results == {
            'r1': '16',
            'r2': '27',
            'r3': '256'
        }


class TestErrorHandling(BaseMultipleFunctionsParallelErrorHandlingTestCase):
    def test_error_handling_without_silent_propagates(self):
        params = {
            'r1': error_function_decorated.future("Test 1"),
            'r2': error_function_decorated.future("Test 2"),
        }
        with parallel.async_par(params) as ex:
            with pytest.raises(TestingException):
                ex.results()

    def test_error_handling_silent_with_params(self):
        params = {
            'r1': add_sleep_decorated.future(.05, .03),
            'r2': error_function_decorated.future('Test 1'),
            'r3': multiply_sleep_decorated.future(.05, 2)
        }
        with parallel.async_par(params) as ex:
            results = ex.results(silent=True)

        assert results.failures is True

        assert results == {
            'r1': '0.08',
            'r2': parallel.FailedTask(
                params=('Test 1', ),
                ex=TestingException('Error: Test 1')
            ),
            'r3': '0.1'
        }

        assert results.succeeded == {
            'r1': '0.08',
            'r3': '0.1'
        }
        assert results.failed == {
            'r2': parallel.FailedTask(
                params=('Test 1', ),
                ex=TestingException('Error: Test 1')
            )
        }
        assert results.replace_failed(None) == {
            'r1': '0.08',
            'r2': None,
            'r3': '0.1'
        }


class TestOptions(BaseMultipleFunctionParallelOtherOptionsTestCase):
    def test_options_timeout_modifier(self):
        params = {
            'r1': add_sleep_decorated.future(.5, 1)
        }
        with parallel.async_par(params) as ex:
            with pytest.raises(TimeoutException):
                ex.results(timeout=1)

    def test_options_max_workers_modifier(self):
        params = {
            'r1': (add_sleep_decorated, .05, .03),
            'r2': (multiply_sleep_decorated, .05, 2),
            'r3': (power_decorated, 3)
        }
        with parallel.async_par(params, max_workers=1) as ex:
            results = ex.results()

        assert results == {
            'r1': '0.08',
            'r2': '0.1',
            'r3': '9'
        }

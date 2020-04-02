import time
import pytest

import parallel
from parallel.exceptions import TimeoutException


class TestingException(Exception):
    def __eq__(self, other):
        return type(self) == type(other) and self.args == other.args


def ten():
    return 10


@parallel.decorate
def ten_decorated():
    return 10


@parallel.decorate
def add_sleep_decorated(a, b):
    time.sleep(a + b)
    return str(a + b)


def add_sleep(a, b):
    time.sleep(a + b)
    return str(a + b)


@parallel.decorate
def multiply_sleep_decorated(a, b):
    time.sleep(a * b)
    return str(a * b)


def multiply_sleep(a, b):
    time.sleep(a * b)
    return str(a * b)


@parallel.decorate
def power_decorated(x, power=2, sleep=0.1):
    time.sleep(sleep)
    return str(x ** power)


def power(x, power=2, sleep=0.1):
    time.sleep(sleep)
    return str(x ** power)


@parallel.decorate
def error_function_decorated(param):
    raise TestingException("Error: {}".format(param))


def error_function(param):
    raise TestingException("Error: {}".format(param))


class BaseSameFunctionParallelTestInvokationTestCase:
    def test_basic_function_no_parameters(self):
        raise NotImplementedError()

    def test_basic_function_single_parameter(self):
        raise NotImplementedError()

    def test_basic_multiple_parameters(self):
        raise NotImplementedError()

    def test_parameters_and_named_arguments(self):
        raise NotImplementedError()

    def test_parameters_and_extra_argument(self):
        raise NotImplementedError()

    def test_parameters_named_arguments_and_extras(self):
        raise NotImplementedError()

    def test_parameters_named_arguments_replaces_extras(self):
        raise NotImplementedError()

    def test_parallel_future_with_params_and_named_arguments(self):
        raise NotImplementedError()

    def test_parallel_future_and_sequence_params(self):
        raise NotImplementedError()

    def test_parallel_future_with_params_and_named_arguments_and_extras(self):
        raise NotImplementedError()


class BaseMultipleFunctionsParallelErrorHandlingTestCase:
    def test_error_handling_without_silent_propagates(self):
        raise NotImplementedError()

    def test_error_handling_silent_with_params(self):
        raise NotImplementedError()


class BaseMultipleFunctionParallelOtherOptionsTestCase:
    def test_options_timeout_modifier(self):
        raise NotImplementedError()

    def test_options_max_workers_modifier(self):
        raise NotImplementedError()

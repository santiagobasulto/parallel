import time
import pytest

import parallel
from parallel.exceptions import TimeoutException

from .base import (
    BaseSameFunctionParallelInvokationTestCase,
    BaseSameFunctionParallelErrorHandlingTestCase,
    BaseSameFunctionParallelOtherOptionsTestCase,
    TestingException)

class TestInvokation(BaseSameFunctionParallelInvokationTestCase):
    def test_function_default_behavior(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        assert sleep_return(.1, 'J') == 'J'

    def test_basic_function_single_parameter(self):
        @parallel.decorate
        def sleep_return(sleep):
            time.sleep(sleep)
            return str(sleep)

        results = sleep_return.map({
            'r1': .2,
            'r2': .3,
            'r3': .1,
        })
        assert results == {
            'r1': '0.2',
            'r2': '0.3',
            'r3': '0.1',
        }

    def test_basic_multiple_parameters(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        results = sleep_return.map({
            'r1': (.2, 'a'),
            'r2': (.3, 'b'),
            'r3': (.1, 'c'),
        })
        assert results == {
            'r1': 'a',
            'r2': 'b',
            'r3': 'c',
        }

    def test_parameters_and_named_arguments(self):
        @parallel.decorate
        def sleep_return(sleep, **kwargs):
            result = kwargs['result']
            uppercase = kwargs.get('uppercase', False)
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': (.2, {'result': 'a'}),
            'r2': (.3, {'result': 'b', 'uppercase': True}),
            'r3': (.1, {'result': 'c'}),
        })

        assert results == {
            'r1': 'a',
            'r2': 'B',
            'r3': 'c',
        }

    def test_parameters_and_extra_argument(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': (.2, 'a'),
            'r2': (.3, 'b'),
            'r3': (.1, 'c'),
        }, extras={
            'uppercase': True
        })

        assert results == {
            'r1': 'A',
            'r2': 'B',
            'r3': 'C',
        }

    def test_parameters_named_arguments_and_extras(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': (.2, {'result': 'a'}),
            'r2': (.3, {'result': 'b'}),
            'r3': (.1, {'result': 'c'}),
        }, extras={
            'uppercase': True
        })

        assert results == {
            'r1': 'A',
            'r2': 'B',
            'r3': 'C',
        }

    def test_parameters_named_arguments_replaces_extras(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': (.2, {'result': 'a'}),
            'r2': (.3, {'result': 'b', 'uppercase': False}),
            'r3': (.1, {'result': 'c'}),
        }, extras={
            'uppercase': True
        })

        assert results == {
            'r1': 'A',
            'r2': 'b',
            'r3': 'C',
        }

    def test_parameters_named_arguments_and_extras_using_kwargs(self):
        @parallel.decorate
        def sleep_return(sleep, **kwargs):
            result = kwargs['result']
            uppercase = kwargs.get('uppercase', False)
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': (.2, {'result': 'a'}),
            'r2': (.3, {'result': 'b'}),
            'r3': (.1, {'result': 'c'}),
        }, extras={
            'uppercase': True
        })

        assert results == {
            'r1': 'A',
            'r2': 'B',
            'r3': 'C',
        }

    def test_parameters_not_unpacked(self):
        @parallel.decorate
        def sleep_return(a_tuple):
            sleep, result = a_tuple
            time.sleep(sleep)
            return result

        results = sleep_return.map({
            'r1': (.2, 'a'),
            'r2': (.3, 'b'),
            'r3': (.1, 'c'),
        }, unpack_arguments=False)

        assert results == {
            'r1': 'a',
            'r2': 'b',
            'r3': 'c',
        }

    def test_parameters_not_unpacked_and_extra_argument(self):
        @parallel.decorate
        def sleep_return(a_tuple, uppercase=False):
            sleep, result = a_tuple
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': (.2, 'a'),
            'r2': (.3, 'b'),
            'r3': (.1, 'c'),
        }, extras={
            'uppercase': True
        }, unpack_arguments=False)

        assert results == {
            'r1': 'A',
            'r2': 'B',
            'r3': 'C',
        }

    def test_only_named_arguments_wrapped_in_sequence(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': [{'sleep': .2, 'result': 'a', 'uppercase': False}],
            'r2': [{'sleep': .3, 'result': 'b', 'uppercase': True}],
            'r3': [{'sleep': .1, 'result': 'c', 'uppercase': False}],
        })

        assert results == {
            'r1': 'a',
            'r2': 'B',
            'r3': 'c',
        }

    def test_only_named_arguments_without_sequence_wrapping(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': {'sleep': .2, 'result': 'a', 'uppercase': False},
            'r2': {'sleep': .3, 'result': 'b', 'uppercase': True},
            'r3': {'sleep': .1, 'result': 'c', 'uppercase': False},
        })

        assert results == {
            'r1': 'a',
            'r2': 'B',
            'r3': 'c',
        }

    def test_parallel_arg_with_params_and_named_arguments(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': parallel.arg(.2, result='a'),
            'r2': parallel.arg(.3, result='b', uppercase=True),
            'r3': parallel.arg(.1, result='c')
        })

        assert results == {
            'r1': 'a',
            'r2': 'B',
            'r3': 'c',
        }

    def test_parallel_arg_with_params_and_named_arguments_and_extras(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': parallel.arg(.2, result='a'),
            'r2': parallel.arg(.3, result='b'),
            'r3': parallel.arg(.1, result='c')
        }, extras={
            'uppercase': True
        })

        assert results == {
            'r1': 'A',
            'r2': 'B',
            'r3': 'C',
        }

    def test_parallel_arg_explicit_parameter_replaces_extras(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
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

    def test_parallel_arg_mixed_with_sequence_and_dictionary(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': (.2, 'a'),
            'r2': parallel.arg(.3, result='b', uppercase=True),
            'r3': (.1, {'result': 'c'}),
            'r4': {'sleep': .1,'result': 'd', 'uppercase': True},
            'r5': (.2, {'result': 'e', 'uppercase': True})
        })

        assert results == {
            'r1': 'a',
            'r2': 'B',
            'r3': 'c',
            'r4': 'D',
            'r5': 'E',
        }

class TestErrorHandling(BaseSameFunctionParallelErrorHandlingTestCase):
    def test_error_handling_without_silent_propagates(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            if type(sleep) == str:
                raise TestingException(sleep)
            time.sleep(sleep)
            return result

        with pytest.raises(TestingException):
            sleep_return.map({
                'r1': (.2, 'a'),
                'r2': ('Will Fail', 'b'),
                'r3': (.1, 'c')
            })

    def test_error_handling_silent_with_params(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            if type(sleep) == str:
                raise TestingException(sleep)
            time.sleep(sleep)
            return result

        results = sleep_return.map({
            'r1': (.2, 'a'),
            'r2': ('Will Fail', 'b'),
            'r3': (.1, 'c')
        }, silent=True)

        assert results.failures is True

        assert results == {
            'r1': 'a',
            'r2': parallel.FailedTask(
                params=('Will Fail', 'b'),
                ex=TestingException('Will Fail')
            ),
            'r3': 'c'
        }

        assert results.succeeded == {
            'r1': 'a',
            'r3': 'c'
        }
        assert results.failed == {
            'r2': parallel.FailedTask(
                params=('Will Fail', 'b'),
                ex=TestingException('Will Fail'))
        }
        assert results.replace_failed(None) == {
            'r1': 'a',
            'r2': None,
            'r3': 'c'
        }

    def test_error_handling_silent_with_params_and_named(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            if type(sleep) == str:
                raise TestingException(sleep)
            time.sleep(sleep)
            return result

        results = sleep_return.map({
            'r1': (.2, 'a'),
            'r2': ('Will Fail 1', 'b'),
            'r3': ('Will Fail 2', {'result': 'c'}),
            'r4': {'sleep': 'Will Fail 3', 'result': 'd'},
            'r5': (.1, {'result': 'e'})
        }, silent=True)

        assert results.failures is True

        assert results == {
            'r1': 'a',
            'r2': parallel.FailedTask(
                params=('Will Fail 1', 'b'),
                ex=TestingException('Will Fail 1')
            ),
            'r3': parallel.FailedTask(
                params=('Will Fail 2',),
                kwargs={'result': 'c'},
                ex=TestingException('Will Fail 2')
            ),
            'r4': parallel.FailedTask(
                kwargs={'sleep': 'Will Fail 3', 'result': 'd'},
                ex=TestingException('Will Fail 3')
            ),
            'r5': 'e'
        }

    def test_error_handling_silent_with_params_and_parallel_arg(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            if type(sleep) == str:
                raise TestingException(sleep)
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        results = sleep_return.map({
            'r1': (.2, 'a'),
            'r2': ('Will Fail 1', 'b'),
            'r3': parallel.arg('Will Fail 2', 'c'),
            'r4': parallel.arg('Will Fail 3', 'd', uppercase=True),
            'r5': parallel.arg(.1, 'e')
        }, silent=True)

        assert results.failures is True

        assert results == {
            'r1': 'a',
            'r2': parallel.FailedTask(
                params=('Will Fail 1', 'b'),
                ex=TestingException('Will Fail 1')
            ),
            'r3': parallel.FailedTask(
                params=('Will Fail 2', 'c'),
                ex=TestingException('Will Fail 2')
            ),
            'r4': parallel.FailedTask(
                params=('Will Fail 3', 'd'),
                kwargs={'uppercase': True},
                ex=TestingException('Will Fail 3')
            ),
            'r5': 'e'
        }

    def test_error_handling_timeout_errors_silent(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        results = sleep_return.map({
            'r1': (1, 'a'),
            'r2': (.2, 'c')
        }, silent=True, timeout=.5)

        assert results.failures is True

        assert results == {
            'r1': parallel.FailedTask(
                params=(1, 'a'),
                ex=TimeoutException()
            ),
            'r2': 'c'
        }

        assert results['r1'] == parallel.FailedTask(
            params=(1, 'a'),
            ex=TimeoutException()
        )
        assert results['r2'] == 'c'

        assert results.succeeded == {'r2': 'c'}
        assert results.failed == {
            'r1': parallel.FailedTask(
                params=(1, 'a'),
                ex=TimeoutException())
        }
        assert results.replace_failed(None) == {
            'r1': None,
            'r2': 'c'
        }


class TestOptions(BaseSameFunctionParallelOtherOptionsTestCase):
    def test_options_timeout_decorator_option(self):
        @parallel.decorate(timeout=1)
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        with pytest.raises(TimeoutException):
            results = sleep_return.map({
                'r1': (3, 'a'),
                'r2': (.3, 'b')
            })

    def test_options_timeout_map_modifier(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        with pytest.raises(TimeoutException):
            results = sleep_return.map({
                'r1': (3, 'a'),
                'r2': (.3, 'b')
            }, timeout=1)

    def test_options_max_workers_decorator_option(self):
        @parallel.decorate(max_workers=1)
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        results = sleep_return.map({
            'r1': (.2, 'a'),
            'r2': (.3, 'b'),
            'r3': (.1, 'c')
        })

        assert results == {
            'r1': 'a',
            'r2': 'b',
            'r3': 'c'
        }

    def test_options_max_workers_map_modifier(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        results = sleep_return.map({
            'r1': (.2, 'a'),
            'r2': (.3, 'b'),
            'r3': (.1, 'c')
        }, max_workers=1)

        assert results == {
            'r1': 'a',
            'r2': 'b',
            'r3': 'c'
        }

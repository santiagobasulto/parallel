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

        with sleep_return.async_map([.2, .3, .1]) as ex:
            results = ex.results()
        assert results == ['0.2', '0.3', '0.1']

        ex = sleep_return.async_map([.2, .3, .1])
        results = ex.results()
        ex.shutdown()

        assert results == ['0.2', '0.3', '0.1']

    def test_basic_multiple_parameters(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        with sleep_return.async_map([(.2, 'a'), (.3, 'b'), (.1, 'c')]) as ex:
            results = ex.results()

        assert results == ['a', 'b', 'c']

        ex = sleep_return.async_map([(.2, 'a'), (.3, 'b'), (.1, 'c')])
        results = ex.results()
        ex.shutdown()

        assert results == ['a', 'b', 'c']

    def test_parameters_and_named_arguments(self):
        @parallel.decorate
        def sleep_return(sleep, **kwargs):
            result = kwargs['result']
            uppercase = kwargs.get('uppercase', False)
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            (.2, {'result': 'a'}),
            (.3, {'result': 'b', 'uppercase': True}),
            (.1, {'result': 'c'}),
        ]
        with sleep_return.async_map(params) as ex:
            results = ex.results()

        assert results == ['a', 'B', 'c']

        ex = sleep_return.async_map(params)
        results = ex.results()
        ex.shutdown()

        assert results == ['a', 'B', 'c']

    def test_parameters_and_extra_argument(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            (.2, 'a'), (.3, 'b'), (.1, 'c')
        ]
        extras = {
            'uppercase': True
        }
        with sleep_return.async_map(params, extras=extras) as ex:
            results = ex.results()

        assert results == ['A', 'B', 'C']

        ex = sleep_return.async_map(params, extras=extras)
        results = ex.results()
        ex.shutdown()

        assert results == ['A', 'B', 'C']

    def test_parameters_named_arguments_and_extras(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            (.2, {'result': 'a'}),
            (.3, {'result': 'b'}),
            (.1, {'result': 'c'}),
        ]
        extras = {
            'uppercase': True
        }
        with sleep_return.async_map(params, extras=extras) as ex:
            results = ex.results()

        assert results == ['A', 'B', 'C']

        ex = sleep_return.async_map(params, extras=extras)
        results = ex.results()
        ex.shutdown()

        assert results == ['A', 'B', 'C']

    def test_parameters_named_arguments_replaces_extras(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            (.2, {'result': 'a'}),
            (.3, {'result': 'b', 'uppercase': False}),
            (.1, {'result': 'c'}),
        ]
        extras = {
            'uppercase': True
        }
        with sleep_return.async_map(params, extras=extras) as ex:
            results = ex.results()

        assert results == ['A', 'b', 'C']

        ex = sleep_return.async_map(params, extras=extras)
        results = ex.results()
        ex.shutdown()

        assert results == ['A', 'b', 'C']

    def test_parameters_named_arguments_and_extras_using_kwargs(self):
        @parallel.decorate
        def sleep_return(sleep, **kwargs):
            result = kwargs['result']
            uppercase = kwargs.get('uppercase', False)
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            (.2, {'result': 'a'}),
            (.3, {'result': 'b'}),
            (.1, {'result': 'c'}),
        ]
        extras = {
            'uppercase': True
        }
        with sleep_return.async_map(params, extras=extras) as ex:
            results = ex.results()

        assert results == ['A', 'B', 'C']

        ex = sleep_return.async_map(params, extras=extras)
        results = ex.results()
        ex.shutdown()

        assert results == ['A', 'B', 'C']

    def test_parameters_not_unpacked(self):
        @parallel.decorate
        def sleep_return(a_tuple):
            sleep, result = a_tuple
            time.sleep(sleep)
            return result

        params = [
            (.2, 'a'), (.3, 'b'), (.1, 'c')
        ]
        with sleep_return.async_map(params, unpack_arguments=False) as ex:
            results = ex.results()

        assert results == ['a', 'b', 'c']

        ex = sleep_return.async_map(params, unpack_arguments=False)
        results = ex.results()
        ex.shutdown()

        assert results == ['a', 'b', 'c']

    def test_parameters_not_unpacked_and_extra_argument(self):
        @parallel.decorate
        def sleep_return(a_tuple, uppercase=False):
            sleep, result = a_tuple
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            (.2, 'a'), (.3, 'b'), (.1, 'c')
        ]
        extras = {
            'uppercase': True
        }
        with sleep_return.async_map(params, extras=extras, unpack_arguments=False) as ex:
            results = ex.results()

        assert results == ['A', 'B', 'C']

        ex = sleep_return.async_map(
            params, extras=extras, unpack_arguments=False)
        results = ex.results()
        ex.shutdown()

        assert results == ['A', 'B', 'C']

    def test_only_named_arguments_wrapped_in_sequence(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            [{'sleep': .2, 'result': 'a', 'uppercase': False}],
            [{'sleep': .3, 'result': 'b', 'uppercase': True}],
            [{'sleep': .1, 'result': 'c', 'uppercase': False}],
        ]
        with sleep_return.async_map(params) as ex:
            results = ex.results()

        assert results == ['a', 'B', 'c']

        ex = sleep_return.async_map(params)
        results = ex.results()
        ex.shutdown()

        assert results == ['a', 'B', 'c']

    def test_only_named_arguments_without_sequence_wrapping(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            {'sleep': .2, 'result': 'a', 'uppercase': False},
            {'sleep': .3, 'result': 'b', 'uppercase': True},
            {'sleep': .1, 'result': 'c', 'uppercase': False},
        ]
        with sleep_return.async_map(params) as ex:
            results = ex.results()

        assert results == ['a', 'B', 'c']

        ex = sleep_return.async_map(params)
        results = ex.results()
        ex.shutdown()

        assert results == ['a', 'B', 'c']

    def test_parallel_arg_with_params_and_named_arguments(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            parallel.arg(.2, result='a'),
            parallel.arg(.3, result='b', uppercase=True),
            parallel.arg(.1, result='c')
        ]
        with sleep_return.async_map(params) as ex:
            results = ex.results()

        assert results == ['a', 'B', 'c']

        ex = sleep_return.async_map(params)
        results = ex.results()
        ex.shutdown()

        assert results == ['a', 'B', 'c']

    def test_parallel_arg_with_params_and_named_arguments_and_extras(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            parallel.arg(.2, result='a'),
            parallel.arg(.3, result='b'),
            parallel.arg(.1, result='c')
        ]
        extras = {
            'uppercase': True
        }
        with sleep_return.async_map(params, extras=extras) as ex:
            results = ex.results()

        assert results == ['A', 'B', 'C']

        ex = sleep_return.async_map(params, extras=extras)
        results = ex.results()
        ex.shutdown()

        assert results == ['A', 'B', 'C']

    def test_parallel_arg_explicit_parameter_replaces_extras(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            parallel.arg(.2, result='a'),
            parallel.arg(.3, result='b', uppercase=False),
            parallel.arg(.1, result='c')
        ]
        extras = {
            'uppercase': True
        }
        with sleep_return.async_map(params, extras=extras) as ex:
            results = ex.results()

        assert results == ['A', 'b', 'C']

        ex = sleep_return.async_map(params, extras=extras)
        results = ex.results()
        ex.shutdown()

        assert results == ['A', 'b', 'C']

    def test_parallel_arg_mixed_with_sequence_and_dictionary(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            (.2, 'a'),
            parallel.arg(.3, result='b', uppercase=True),
            (.1, {'result': 'c'}),
            {'sleep': .1,'result': 'd', 'uppercase': True},
            (.2, {'result': 'e', 'uppercase': True})
        ]
        with sleep_return.async_map(params) as ex:
            results = ex.results()

        assert results == ['a', 'B', 'c', 'D', 'E']

        ex = sleep_return.async_map(params)
        results = ex.results()
        ex.shutdown()

        assert results == ['a', 'B', 'c', 'D', 'E']


class TestErrorHandling(BaseSameFunctionParallelErrorHandlingTestCase):
    def test_error_handling_without_silent_propagates(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            if type(sleep) == str:
                raise TestingException(sleep)
            time.sleep(sleep)
            return result

        params = [
            (.2, 'a'), ('Will Fail', 'b'), (.1, 'c')
        ]
        with sleep_return.async_map(params) as ex:
            with pytest.raises(TestingException):
                ex.results()

        ex = sleep_return.async_map(params)
        with pytest.raises(TestingException):
            ex.results()

    def test_error_handling_silent_with_params(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            if type(sleep) == str:
                raise TestingException(sleep)
            time.sleep(sleep)
            return result

        params = [
            (.2, 'a'), ('Will Fail', 'b'), (.1, 'c')
        ]
        with sleep_return.async_map(params) as ex:
            results = ex.results(silent=True)

        assert results.failures is True

        assert results == [
            'a',
            parallel.FailedTask(
                params=('Will Fail', 'b'),
                ex=TestingException('Will Fail')
            ),
            'c'
        ]
        a, b, c = results
        assert a == 'a'
        assert b == parallel.FailedTask(
            params=('Will Fail', 'b'),
            ex=TestingException('Will Fail')
        )
        assert c == 'c'

        assert results.succeeded == ['a', 'c']
        assert results.failed == [parallel.FailedTask(
            params=('Will Fail', 'b'),
            ex=TestingException('Will Fail')
        )]
        assert results.replace_failed(None) == ['a', None, 'c']

        ex = sleep_return.async_map(params)
        results = ex.results(silent=True)
        ex.shutdown()

        assert results.failures is True

        assert results == [
            'a',
            parallel.FailedTask(
                params=('Will Fail', 'b'),
                ex=TestingException('Will Fail')
            ),
            'c'
        ]
        a, b, c = results
        assert a == 'a'
        assert b == parallel.FailedTask(
            params=('Will Fail', 'b'),
            ex=TestingException('Will Fail')
        )
        assert c == 'c'

        assert results.succeeded == ['a', 'c']
        assert results.failed == [parallel.FailedTask(
            params=('Will Fail', 'b'),
            ex=TestingException('Will Fail')
        )]
        assert results.replace_failed(None) == ['a', None, 'c']

    def test_error_handling_silent_with_params_and_named(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            if type(sleep) == str:
                raise TestingException(sleep)
            time.sleep(sleep)
            return result

        params = [
            (.2, 'a'),
            ('Will Fail 1', 'b'),
            ('Will Fail 2', {'result': 'c'}),
            {'sleep': 'Will Fail 3', 'result': 'd'},
            (.1, {'result': 'e'})
        ]
        with sleep_return.async_map(params) as ex:
            results = ex.results(silent=True)

        assert results.failures is True

        assert results == [
            'a',
            parallel.FailedTask(
                params=('Will Fail 1', 'b'),
                ex=TestingException('Will Fail 1')
            ),
            parallel.FailedTask(
                params=('Will Fail 2',),
                kwargs={'result': 'c'},
                ex=TestingException('Will Fail 2')
            ),
            parallel.FailedTask(
                kwargs={'sleep': 'Will Fail 3', 'result': 'd'},
                ex=TestingException('Will Fail 3')
            ),
            'e'
        ]

        ex = sleep_return.async_map(params)
        results = ex.results(silent=True)
        ex.shutdown()

        assert results.failures is True

        assert results == [
            'a',
            parallel.FailedTask(
                params=('Will Fail 1', 'b'),
                ex=TestingException('Will Fail 1')
            ),
            parallel.FailedTask(
                params=('Will Fail 2',),
                kwargs={'result': 'c'},
                ex=TestingException('Will Fail 2')
            ),
            parallel.FailedTask(
                kwargs={'sleep': 'Will Fail 3', 'result': 'd'},
                ex=TestingException('Will Fail 3')
            ),
            'e'
        ]

    def test_error_handling_silent_with_params_and_parallel_arg(self):
        @parallel.decorate
        def sleep_return(sleep, result, uppercase=False):
            if type(sleep) == str:
                raise TestingException(sleep)
            time.sleep(sleep)
            if uppercase:
                return result.upper()
            return result

        params = [
            (.2, 'a'),
            ('Will Fail 1', 'b'),
            parallel.arg('Will Fail 2', 'c'),
            parallel.arg('Will Fail 3', 'd', uppercase=True),
            parallel.arg(.1, 'e')
        ]
        with sleep_return.async_map(params) as ex:
            results = ex.results(silent=True)

        assert results.failures is True

        assert results == [
            'a',
            parallel.FailedTask(
                params=('Will Fail 1', 'b'),
                ex=TestingException('Will Fail 1')
            ),
            parallel.FailedTask(
                params=('Will Fail 2', 'c'),
                ex=TestingException('Will Fail 2')
            ),
            parallel.FailedTask(
                params=('Will Fail 3', 'd'),
                kwargs={'uppercase': True},
                ex=TestingException('Will Fail 3')
            ),
            'e'
        ]

        ex = sleep_return.async_map(params)
        results = ex.results(silent=True)
        ex.shutdown()

        assert results.failures is True

        assert results == [
            'a',
            parallel.FailedTask(
                params=('Will Fail 1', 'b'),
                ex=TestingException('Will Fail 1')
            ),
            parallel.FailedTask(
                params=('Will Fail 2', 'c'),
                ex=TestingException('Will Fail 2')
            ),
            parallel.FailedTask(
                params=('Will Fail 3', 'd'),
                kwargs={'uppercase': True},
                ex=TestingException('Will Fail 3')
            ),
            'e'
        ]

    def test_error_handling_timeout_errors_silent(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        params = [(1, 'a'), (.2, 'c')]
        with sleep_return.async_map(params) as ex:
            results = ex.results(silent=True, timeout=.5)

        assert results.failures is True

        assert results == [
            parallel.FailedTask(
                params=(1, 'a'),
                ex=TimeoutException()
            ),
            'c'
        ]
        a, c = results
        assert a == parallel.FailedTask(
            params=(1, 'a'),
            ex=TimeoutException()
        )
        assert c == 'c'

        assert results.succeeded == ['c']
        assert results.failed == [parallel.FailedTask(
            params=(1, 'a'),
            ex=TimeoutException()
        )]
        assert results.replace_failed(None) == [None, 'c']

        ex = sleep_return.async_map(params)
        results = ex.results(silent=True, timeout=.5)
        ex.shutdown()

        assert results.failures is True

        assert results == [
            parallel.FailedTask(
                params=(1, 'a'),
                ex=TimeoutException()
            ),
            'c'
        ]
        a, c = results
        assert a == parallel.FailedTask(
            params=(1, 'a'),
            ex=TimeoutException()
        )
        assert c == 'c'

        assert results.succeeded == ['c']
        assert results.failed == [parallel.FailedTask(
            params=(1, 'a'),
            ex=TimeoutException()
        )]
        assert results.replace_failed(None) == [None, 'c']


class TestOptions(BaseSameFunctionParallelOtherOptionsTestCase):
    def test_options_timeout_decorator_option(self):
        @parallel.decorate(timeout=1)
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        params = [(3, 'a'), (.3, 'b')]
        with sleep_return.async_map(params) as ex:
            with pytest.raises(TimeoutException):
                ex.results()

        ex = sleep_return.async_map(params)
        with pytest.raises(TimeoutException):
            ex.results()
        ex.shutdown()

    def test_options_timeout_map_modifier(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        params = [(3, 'a'), (.3, 'b')]
        with sleep_return.async_map(params) as ex:
            with pytest.raises(TimeoutException):
                ex.results(timeout=1)

        ex = sleep_return.async_map(params)
        with pytest.raises(TimeoutException):
            ex.results(timeout=1)
        ex.shutdown()

    def test_options_max_workers_decorator_option(self):
        @parallel.decorate(max_workers=1)
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        params = [
            (.2, 'a'), (.3, 'b'), (.1, 'c')
        ]
        with sleep_return.async_map(params) as ex:
            results = ex.results()

        assert results == ['a', 'b', 'c']

        ex = sleep_return.async_map(params)
        results = ex.results()
        ex.shutdown()

        assert results == ['a', 'b', 'c']

    def test_options_max_workers_map_modifier(self):
        @parallel.decorate
        def sleep_return(sleep, result):
            time.sleep(sleep)
            return result

        params = [
            (.2, 'a'), (.3, 'b'), (.1, 'c')
        ]
        with sleep_return.async_map(params, max_workers=1) as ex:
            results = ex.results()

        assert results == ['a', 'b', 'c']

        ex = sleep_return.async_map(params, max_workers=1)
        results = ex.results()
        ex.shutdown()

        assert results == ['a', 'b', 'c']

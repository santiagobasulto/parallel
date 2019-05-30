import time
import pytest

import parallel
from parallel.exceptions import TimeoutException


class TestingException(Exception):
    def __eq__(self, other):
        return type(self) == type(other) and self.args == other.args


class BaseSameFunctionParallelInvokationTestCase:
    def test_function_default_behavior(self):
        "Should run normal function when invoked"
        raise NotImplementedError()

    def test_basic_function_single_parameter(self):
        "Should run test in parallel with just one parameter"
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

    def test_parameters_named_arguments_and_extras_using_kwargs(self):
        raise NotImplementedError()

    def test_parameters_not_unpacked(self):
        raise NotImplementedError()

    def test_parameters_not_unpacked_and_extra_argument(self):
        raise NotImplementedError()

    def test_only_named_arguments_wrapped_in_sequence(self):
        raise NotImplementedError()

    def test_only_named_arguments_without_sequence_wrapping(self):
        raise NotImplementedError()

    def test_parallel_arg_with_params_and_named_arguments(self):
        raise NotImplementedError()

    def test_parallel_arg_with_params_and_named_arguments_and_extras(self):
        raise NotImplementedError()

    def test_parallel_arg_explicit_parameter_replaces_extras(self):
        raise NotImplementedError()

    def test_parallel_arg_mixed_with_sequence_and_dictionary(self):
        raise NotImplementedError()


class BaseSameFunctionParallelErrorHandlingTestCase:
    def test_error_handling_without_silent_propagates(self):
        raise NotImplementedError()

    def test_error_handling_silent_with_params(self):
        raise NotImplementedError()

    def test_error_handling_silent_with_params_and_named(self):
        raise NotImplementedError()

    def test_error_handling_silent_with_params_and_parallel_arg(self):
        raise NotImplementedError()

    def test_error_handling_timeout_errors_silent(self):
        raise NotImplementedError()


class BaseSameFunctionParallelOtherOptionsTestCase:
    def test_options_timeout_decorator_option(self):
        raise NotImplementedError()

    def test_options_timeout_map_modifier(self):
        raise NotImplementedError()

    def test_options_max_workers_decorator_option(self):
        raise NotImplementedError()

    def test_options_max_workers_map_modifier(self):
        raise NotImplementedError()


# class BaseSameFunctionParallelOtherExecutorTestCase:
#     def test_parallelize_function_executor_thread_decorator_param():
#         @parallel.decorate(ex=parallel.THREAD)  # default
#         def sleep_return(sleep, result):
#             time.sleep(sleep)
#             return result
#
#         results = sleep_return.map([
#             (2, 'a'), (.3, 'b'), (1, 'c')
#         ])
#
#         assert results == ['a', 'b', 'c']
#
#
#     def test_parallelize_function_executor_thread_modifier():
#         @parallel.decorate  # default
#         def sleep_return(sleep, result):
#             time.sleep(sleep)
#             return result
#
#         results = sleep_return.thread.map([
#             (2, 'a'), (.3, 'b'), (1, 'c')
#         ])
#
#         assert results == ['a', 'b', 'c']

# # TODO: Process...
# # @parallel.decorate(ex=parallel.PROCESS)
# # def sleep_return_process(sleep, result):
# #     time.sleep(sleep)
# #     return result
# #
# #
#
# #
# # def test_parallelize_function_executor_process_decorator_param():
# #     results = sleep_return_process.map([
# #         (2, 'a'), (.3, 'b'), (1, 'c')
# #     ])
# #
# #     assert results == ['a', 'b', 'c']
#
# def sleep_return_process_not_decorated(sleep, result):
#     time.sleep(sleep)
#     return result
#
#
# def test_parallelize_function_executor_process_decorator_param():
#     callable = parallel.ParallelCallable(
#         sleep_return_process_not_decorated, ex=parallel.PROCESS)
#
#     # results = sleep_return_process.map([
#     #     (2, 'a'), (.3, 'b'), (1, 'c')
#     # ])
#     results = callable.map([
#         (2, 'a'), (.3, 'b'), (1, 'c')
#     ])
#
#     assert results == ['a', 'b', 'c']
#
# # TODO: Process
# # def test_parallelize_function_executor_process_modifier():
# #     @parallel.decorate
# #     def sleep_return(sleep, result):
# #         time.sleep(sleep)
# #         return result
# #
# #     results = sleep_return.process.map([
# #         (2, 'a'), (.3, 'b'), (1, 'c')
# #     ])
# #
# #     assert results == ['a', 'b', 'c']

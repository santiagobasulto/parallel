import time

__all__ = [
    'sleep_return_single_param',
    'sleep_return_tuple',
    'sleep_return_multi_param',
    'sleep_return_optional_param',
    'sleep_return_tuple_optional',
    'sleep_return_kwargs',

    'TestingException'
]
class TestingException(Exception):
    def __eq__(self, other):
        return type(self) == type(other) and self.args == other.args

def sleep_return_single_param(sleep):
    time.sleep(sleep)
    return str(sleep)

def sleep_return_tuple(a_tuple):
    sleep, result = a_tuple
    time.sleep(sleep)
    return result

def sleep_return_multi_param(sleep, result):
    if type(sleep) == str:
        raise TestingException(sleep)
    time.sleep(sleep)
    return result

def sleep_return_optional_param(sleep, result, uppercase=False):
    time.sleep(sleep)
    if uppercase:
        return result.upper()
    return result

def sleep_return_tuple_optional(a_tuple, uppercase=False):
    sleep, result = a_tuple
    time.sleep(sleep)
    if uppercase:
        return result.upper()
    return result

def sleep_return_kwargs(sleep, **kwargs):
    result = kwargs['result']
    uppercase = kwargs.get('uppercase', False)
    time.sleep(sleep)
    if uppercase:
        return result.upper()
    return result
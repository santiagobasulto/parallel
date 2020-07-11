import time

import parallel

__all__ = [
    'sleep_return_single_param',
    'sleep_return_tuple',
    'sleep_return_multi_param',
    'sleep_return_optional_param',
    'sleep_return_tuple_optional',
    'sleep_return_kwargs',

    'sleep_return_single_param_decorated',
    'sleep_return_multi_param_decorated',
    'sleep_return_optional_param_decorated',
    'sleep_return_tuple_decorated',
    'sleep_return_single_param_decorated_timeout',
    'sleep_return_multi_param_decorated_max_workers',

    'TestingException'
]

class TestingException(Exception):
    __test__ = False

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


@parallel.decorate
def sleep_return_single_param_decorated(sleep):
    time.sleep(sleep)
    return str(sleep)


@parallel.decorate
def sleep_return_multi_param_decorated(sleep, result):
    if type(sleep) == str:
        raise TestingException(sleep)
    time.sleep(sleep)
    return result


@parallel.decorate
def sleep_return_optional_param_decorated(sleep, result, uppercase=False):
    time.sleep(sleep)
    if uppercase:
        return result.upper()
    return result


@parallel.decorate
def sleep_return_tuple_decorated(a_tuple):
    sleep, result = a_tuple
    time.sleep(sleep)
    return result


@parallel.decorate(timeout=.1)
def sleep_return_single_param_decorated_timeout(sleep):
    time.sleep(sleep)
    return str(sleep)


@parallel.decorate(max_workers=1)
def sleep_return_multi_param_decorated_max_workers(sleep, result):
    if type(sleep) == str:
        raise TestingException(sleep)
    time.sleep(sleep)
    return result
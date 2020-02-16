import pytest

import parallel
from parallel.models import ParallelJob

from ..base import *

# Tests:

####################
# Single Parameter #
####################
def test_async_map_dict_basic_single_param():
    with parallel.async_map(sleep_return_single_param, {
        'r1': .2,
        'r2': .3
    }) as ex:
        assert ex.results() == {
            'r1': '0.2',
            'r2': '0.3'
        }

#######################
# Multiple parameters #
#######################
def test_async_map_dict_basic_multi_param():
    with parallel.async_map(sleep_return_multi_param, {
        'r1': (.2, 'a'),
        'r2': (.3, 'b')
    }) as ex:
        assert ex.results() == {
            'r1': 'a',
            'r2': 'b'
        }


####################
# Named parameters #
####################
def test_async_map_dict_named_parameters():
    with parallel.async_map(sleep_return_multi_param, {
        'r1': (.2, {'result': 'a'}),
        'r2': (.3, {'result': 'b'}),
        'r3': (.1, {'result': 'c'}),
    }) as ex:
        assert ex.results() == {
            'r1': 'a',
            'r2': 'b',
            'r3': 'c',
        }


########

def test_async_without_context_manager():
    ex = parallel.async_map(sleep_return_multi_param, {
        'r1': (.2, 'a'),
        'r2': (.3, 'b')
    })
    ex.start()
    assert ex.results() == {
        'r1': 'a',
        'r2': 'b'
    }
    ex.shutdown()
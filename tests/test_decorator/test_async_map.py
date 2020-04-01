import time
import pytest

import parallel
from parallel.models import ParallelJob

from ..base import *

def test_async_map_dict_basic_single_param():
    with sleep_return_single_param_decorated.async_map({
        'r1': .2,
        'r2': .3
    }) as ex:
        assert ex.results() == {
            'r1': '0.2',
            'r2': '0.3'
        }
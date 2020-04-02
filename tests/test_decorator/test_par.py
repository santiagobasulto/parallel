import time
import pytest

import parallel
from parallel.models import ParallelJob

from ..base import *


def test_par_decorated():
    results = parallel.par({
        'r1': sleep_return_multi_param_decorated.future(.1, 'a'),
        'r2': sleep_return_multi_param_decorated.future(.2, 'b'),
    })
    assert results == {
        'r1': 'a',
        'r2': 'b'
    }
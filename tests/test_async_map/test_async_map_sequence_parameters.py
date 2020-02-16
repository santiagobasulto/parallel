import time
import pytest

import parallel
from parallel.models import ParallelJob

from ..base import *

####################
# Single Parameter #
####################
def test_async_map_sequence_basic_single_param():
    results = parallel.map(sleep_return_single_param, [0.2, 0.3, 0.1])
    assert results == ["0.2", "0.3", "0.1"]

    with parallel.async_map(sleep_return_single_param, [0.2, 0.3, 0.1]) as ex:
        assert ex.results() == ["0.2", "0.3", "0.1"]


#######################
# Multiple parameters #
#######################
def test_async_map_sequence_basic_multi_param():
    with parallel.async_map(
        sleep_return_multi_param, [(0.2, "a"), (0.3, "b"), (0.1, "c")]
    ) as ex:
        assert ex.results() == ["a", "b", "c"]

    # Thread
    with parallel.thread.async_map(
        sleep_return_multi_param, [(0.2, "a"), (0.3, "b"), (0.1, "c")]
    ) as ex:
        assert ex.results() == ["a", "b", "c"]

    # Process
    with parallel.process.async_map(
        sleep_return_multi_param, [(0.2, "a"), (0.3, "b"), (0.1, "c")]
    ) as ex:
        assert ex.results() == ["a", "b", "c"]


####################
# Named parameters #
####################
def test_async_map_sequence_named_parameters():
    with parallel.async_map(
        sleep_return_multi_param,
        [(0.2, {"result": "a"}), (0.3, {"result": "b",}), (0.1, {"result": "c"}),],
    ) as ex:
        assert ex.results() == ["a", "b", "c"]


#####################################
# Only Named parameters in sequence #
#####################################
def test_async_map_sequence_only_named_parameters():
    with parallel.async_map(
        sleep_return_optional_param,
        [
            {"sleep": 0.2, "result": "a", "uppercase": False},
            {"sleep": 0.3, "result": "b", "uppercase": True},
            {"sleep": 0.1, "result": "c", "uppercase": False},
        ],
    ) as ex:
        assert ex.results() == ["a", "B", "c"]



######

def test_async_without_context_manager():
    ex = parallel.async_map(
        sleep_return_multi_param,
        [(0.2, {"result": "a"}), (0.3, {"result": "b",}), (0.1, {"result": "c"}),],
    )
    ex.start()
    assert ex.results() == ["a", "b", "c"]
    ex.shutdown()
import time
import pytest

import parallel
from parallel.models import ParallelJob

from ..base import *

# Tests:

####################
# Single Parameter #
####################
def test_map_sequence_basic_single_param():
    results = parallel.map(sleep_return_single_param, [.2, .3, .1])
    assert results == ['0.2', '0.3', '0.1']

#######################
# Multiple parameters #
#######################
def test_map_sequence_basic_multi_param():
    results = parallel.map(
        sleep_return_multi_param,
        [(.2, 'a'), (.3, 'b'), (.1, 'c')])
    assert results == ['a', 'b', 'c']

    # Thread
    results = parallel.thread.map(
        sleep_return_multi_param,
        [(.2, 'a'), (.3, 'b'), (.1, 'c')])
    assert results == ['a', 'b', 'c']

    # Process
    results = parallel.process.map(
        sleep_return_multi_param,
        [(.2, 'a'), (.3, 'b'), (.1, 'c')])
    assert results == ['a', 'b', 'c']

####################
# Named parameters #
####################
def test_map_sequence_named_parameters():
    results = parallel.map(sleep_return_multi_param, [
        (.2, {'result': 'a'}),
        (.3, {'result': 'b',}),
        (.1, {'result': 'c'}),
    ])
    assert results == ['a', 'b', 'c']

#####################################
# Only Named parameters in sequence #
#####################################
def test_map_sequence_only_named_parameters():
    results = parallel.map(sleep_return_optional_param, [
        {'sleep': .2, 'result': 'a', 'uppercase': False},
        {'sleep': .3, 'result': 'b', 'uppercase': True},
        {'sleep': .1, 'result': 'c', 'uppercase': False},
    ])
    assert results == ['a', 'B', 'c']

###########################################
# Named parameters on a **kwargs function #
###########################################
def test_map_sequence_named_parameters_kwargs():
    results = parallel.map(sleep_return_kwargs, [
        (.2, {'result': 'a'}),
        (.3, {'result': 'b',}),
        (.1, {'result': 'c'}),
    ])
    assert results == ['a', 'b', 'c']


######################
# Optional parameter #
######################
def test_map_sequence_optional_parameters():
    results = parallel.map(sleep_return_optional_param, [
        (.2, {'result': 'a'}),
        (.3, {'result': 'b', 'uppercase': True}),
        (.1, {'result': 'c'}),
    ])
    assert results == ['a', 'B', 'c']


##############################################
# Optional parameters on a **kwargs function #
##############################################
def test_map_sequence_optional_parameters_kwargs():
    results = parallel.map(sleep_return_kwargs, [
        (.2, {'result': 'a'}),
        (.3, {'result': 'b', 'uppercase': True}),
        (.1, {'result': 'c'}),
    ])
    assert results == ['a', 'B', 'c']


################
# Extras Basic #
################
def test_map_sequence_extras_parameters():
    results = parallel.map(sleep_return_optional_param, [
        (.2, 'a'), (.3, 'b'), (.1, 'c')
    ], extras={
        'uppercase': True
    })
    assert results == ['A', 'B', 'C']


###########################
# Extras Named Parameters #
###########################
def test_map_sequence_extras_named_parameters():
    results = parallel.map(sleep_return_kwargs, [
        (.2, {'result': 'a'}),
        (.3, {'result': 'b'}),
        (.1, {'result': 'c'}),
    ], extras={
        'uppercase': True
    })
    assert results == ['A', 'B', 'C']


##################################################
# Extras Parameters overwrite by named parameter #
##################################################
def test_map_sequence_extras_named_parameters_overwrite():
    results = parallel.map(sleep_return_kwargs, [
        (.2, {'result': 'a'}),
        (.3, {'result': 'b', 'uppercase': False}),
        (.1, {'result': 'c'}),
    ], extras={
        'uppercase': True
    })
    assert results == ['A', 'b', 'C']


##########################
# Don't unpack arguments #
##########################
def test_map_sequence_unpack_arguments_false():
    results = parallel.map(sleep_return_tuple, [
        (.2, 'a'), (.3, 'b'), (.1, 'c')
    ], unpack_arguments=False)

    assert results == ['a', 'b', 'c']


###############################################
# Don't unpack arguments & optional parameter #
###############################################
def test_map_sequence_unpack_arguments_false_optional_param():
    results = parallel.map(sleep_return_tuple_optional, [
        (.2, 'a'), (.3, 'b'), (.1, 'c')
    ], extras={
        'uppercase': True
    }, unpack_arguments=False)

    assert results == ['A', 'B', 'C']


#################################################
# parallel.arg with extras and named parameters #
#################################################
def test_map_sequence_arg_extras_named_parameters():
    results = parallel.map(sleep_return_optional_param, [
        parallel.arg(.2, result='a'),
        parallel.arg(.3, result='b', uppercase=False),
        parallel.arg(.1, result='c')
    ], extras={
        'uppercase': True
    })
    assert results == ['A', 'b', 'C']


########################################################
# parallel.arg with extras and named parameters kwargs #
########################################################
def test_map_sequence_arg_extras_named_parameters_kwargs():
    results = parallel.map(sleep_return_kwargs, [
        parallel.arg(.2, result='a'),
        parallel.arg(.3, result='b', uppercase=False),
        parallel.arg(.1, result='c')
    ], extras={
        'uppercase': True
    })
    assert results == ['A', 'b', 'C']


####################################################
# Mixed argument types (tuple, dict, parallel.arg) #
####################################################
def test_map_sequence_mixed_argument_types():
    results = parallel.map(sleep_return_optional_param, [
        (.2, 'a'),
        parallel.arg(.3, result='b', uppercase=False),
        (.1, {'result': 'c'}),
        {'sleep': .1, 'result': 'd', 'uppercase': False},
        (.2, {'result': 'e', 'uppercase': False})
    ], extras={
        'uppercase': True
    })
    assert results == ['A', 'b', 'C', 'd', 'e']

####################################
# Errors propagate if not silenced #
####################################
def test_map_sequence_exceptions_propagated():
    with pytest.raises(TestingException):
        parallel.map(sleep_return_multi_param, [
            (.2, 'a'), ('Will Fail', 'b'), (.1, 'c')
        ])

###################################
# Silence errors with silent=True #
###################################
def test_map_sequence_silent_mode_catches_exceptions():
    results = parallel.map(sleep_return_multi_param, [
        (.2, 'a'), ('Will Fail', 'b'), (.1, 'c')
    ], silent=True)

    assert results.failures is True

    assert results == [
        'a',
        parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, args=('Will Fail', 'b')),
            exc=TestingException('Will Fail')
        ),
        'c'
    ]
    a, b, c = results
    assert a == 'a'
    assert b == parallel.FailedTask(
        ParallelJob(sleep_return_multi_param, args=('Will Fail', 'b')),
        exc=TestingException('Will Fail')
    )
    assert c == 'c'


########################################################
# Silence errors with silent=True and named parameters #
########################################################
def test_map_sequence_silent_mode_catches_exceptions_with_named_parameters():
    results = parallel.map(sleep_return_multi_param, [
        (.2, 'a'),
        ('Will Fail 1', 'b'),
        ('Will Fail 2', {'result': 'c'}),
        {'sleep': 'Will Fail 3', 'result': 'd'},
        (.1, {'result': 'e'}),
        parallel.arg('Will Fail 4', 'f'),
    ], silent=True)

    assert results.failures is True
    expected = [
        'a',
        parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, args=('Will Fail 1', 'b')),
            exc=TestingException('Will Fail 1')
        ),
        parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, args=('Will Fail 2',), kwargs={'result': 'c'}),
            exc=TestingException('Will Fail 2')
        ),
        parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, args=tuple(), kwargs={'sleep': 'Will Fail 3', 'result': 'd'}),
            exc=TestingException('Will Fail 3')
        ),
        'e',
        parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, args=('Will Fail 4', 'f')),
            exc=TestingException('Will Fail 4')
        ),
    ]

    assert results == expected
import pytest

import parallel
from parallel.models import ParallelJob

from ..base import *

# Tests:

####################
# Single Parameter #
####################
def test_map_dict_basic_single_param():
    results = parallel.map(sleep_return_single_param, {
        'r1': .2,
        'r2': .3
    })
    assert results == {
        'r1': '0.2',
        'r2': '0.3'
    }

#######################
# Multiple parameters #
#######################
def test_map_dict_basic_multi_param():
    results = parallel.map(sleep_return_multi_param, {
        'r1': (.2, 'a'),
        'r2': (.3, 'b')
    })
    assert results == {
        'r1': 'a',
        'r2': 'b'
    }

####################
# Named parameters #
####################
def test_map_dict_named_parameters():
    results = parallel.map(sleep_return_multi_param, {
        'r1': (.2, {'result': 'a'}),
        'r2': (.3, {'result': 'b'}),
        'r3': (.1, {'result': 'c'}),
    })
    assert results == {
        'r1': 'a',
        'r2': 'b',
        'r3': 'c',
    }

###########################################
# Named parameters on a **kwargs function #
###########################################
def test_map_dict_named_parameters_kwargs():
    results = parallel.map(sleep_return_kwargs, {
        'r1': (.2, {'result': 'a'}),
        'r2': (.3, {'result': 'b'}),
        'r3': (.1, {'result': 'c'}),
    })
    assert results == {
        'r1': 'a',
        'r2': 'b',
        'r3': 'c',
    }

######################
# Optional parameter #
######################
def test_map_dict_optional_parameters():
    results = parallel.map(sleep_return_optional_param, {
        'r1': (.2, {'result': 'a'}),
        'r2': (.3, {'result': 'b', 'uppercase': True}),
        'r3': (.1, {'result': 'c'}),
    })
    assert results == {
        'r1': 'a',
        'r2': 'B',
        'r3': 'c',
    }

##############################################
# Optional parameters on a **kwargs function #
##############################################
def test_map_dict_optional_parameters_kwargs():
    results = parallel.map(sleep_return_kwargs, {
        'r1': (.2, {'result': 'a'}),
        'r2': (.3, {'result': 'b', 'uppercase': True}),
        'r3': (.1, {'result': 'c'}),
    })
    assert results == {
        'r1': 'a',
        'r2': 'B',
        'r3': 'c',
    }

################
# Extras Basic #
################
def test_map_dict_extras_parameters():
    results = parallel.map(sleep_return_optional_param, {
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

###########################
# Extras Named Parameters #
###########################
def test_map_dict_extras_named_parameters():
    results = parallel.map(sleep_return_kwargs, {
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


##################################################
# Extras Parameters overwrite by named parameter #
##################################################
def test_map_dict_extras_named_parameters_overwrite():

    results = parallel.map(sleep_return_kwargs, {
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


##########################
# Don't unpack arguments #
##########################
def test_map_dict_unpack_arguments_false():
    results = parallel.map(sleep_return_tuple, {
        'r1': (.2, 'a'),
        'r2': (.3, 'b'),
        'r3': (.1, 'c')
    }, unpack_arguments=False)

    assert results == {
        'r1': 'a',
        'r2': 'b',
        'r3': 'c',
    }


###############################################
# Don't unpack arguments & optional parameter #
###############################################
def test_map_dict_unpack_arguments_false_optional_param():
    results = parallel.map(sleep_return_tuple_optional, {
        'r1': (.2, 'a'),
        'r2': (.3, 'b'),
        'r3': (.1, 'c')
    }, extras={
        'uppercase': True
    }, unpack_arguments=False)

    assert results == {
        'r1': 'A',
        'r2': 'B',
        'r3': 'C',
    }


#################################################
# parallel.arg with extras and named parameters #
#################################################
def test_map_dict_arg_extras_named_parameters():
    results = parallel.map(sleep_return_optional_param, {
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

########################################################
# parallel.arg with extras and named parameters kwargs #
########################################################
def test_map_dict_arg_extras_named_parameters_kwargs():
    results = parallel.map(sleep_return_kwargs, {
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


####################################################
# Mixed argument types (tuple, dict, parallel.arg) #
####################################################
def test_map_dict_mixed_argument_types():
    results = parallel.map(sleep_return_optional_param, {
        'r1': (.2, 'a'),
        'r2': parallel.arg(.3, result='b', uppercase=False),
        'r3': (.1, {'result': 'c'}),
        'r4': {'sleep': .1, 'result': 'd', 'uppercase': False},
        'r5': (.2, {'result': 'e', 'uppercase': False})
    }, extras={
        'uppercase': True
    })
    assert results == {
        'r1': 'A',
        'r2': 'b',
        'r3': 'C',
        'r4': 'd',
        'r5': 'e'
    }


####################################
# Errors propagate if not silenced #
####################################
def test_map_dict_exceptions_propagated():
    with pytest.raises(TestingException):
        parallel.map(sleep_return_multi_param, {
            'r1': (.2, 'a'),
            'r2': ('Will Fail', 'b'),
            'r3': (.1, 'c')
        })


###################################
# Silence errors with silent=True #
###################################
def test_map_dict_silent_mode_catches_exceptions():
    results = parallel.map(sleep_return_multi_param, {
            'r1': (.2, 'a'),
            'r2': ('Will Fail', 'b'),
            'r3': (.1, 'c')
        }, silent=True)

    assert results.failures is True

    assert results == {
        'r1': 'a',
        'r2': parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, 'r2', args=('Will Fail', 'b')),
            exc=TestingException('Will Fail')
        ),
        'r3': 'c'
    }


########################################################
# Silence errors with silent=True and named parameters #
########################################################
def test_map_dict_silent_mode_catches_exceptions_with_named_parameters():
    results = parallel.map(sleep_return_multi_param, {
        'r1': (.2, 'a'),
        'r2': ('Will Fail 1', 'b'),
        'r3': ('Will Fail 2', {'result': 'c'}),
        'r4': {'sleep': 'Will Fail 3', 'result': 'd'},
        'r5': (.1, {'result': 'e'}),
        'r6': parallel.arg('Will Fail 4', 'f'),
    }, silent=True)

    assert results.failures is True

    expected = {
        'r1': 'a',
        'r2': parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, 'r2', args=('Will Fail 1', 'b')),
            exc=TestingException('Will Fail 1')
        ),
        'r3': parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, 'r3', args=('Will Fail 2',), kwargs={'result': 'c'}),
            exc=TestingException('Will Fail 2')
        ),
        'r4': parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, 'r4', args=tuple(), kwargs={'sleep': 'Will Fail 3', 'result': 'd'}),
            exc=TestingException('Will Fail 3')
        ),
        'r5': 'e',
        'r6': parallel.FailedTask(
            ParallelJob(sleep_return_multi_param, 'r6', args=('Will Fail 4', 'f')),
            exc=TestingException('Will Fail 4')
        ),
    }

    assert results == expected
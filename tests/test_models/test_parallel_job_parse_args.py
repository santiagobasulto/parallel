from parallel.models import ParallelJob, ParallelArg
from unittest.mock import MagicMock

mocked_fn = MagicMock(return_value=None)

def test_normalize_param():
    args, kwargs = ParallelJob.normalize_params(2)
    assert args == (2, )
    assert kwargs == {}

    args, kwargs = ParallelJob.normalize_params((2, 3))
    assert args == (2, 3)
    assert kwargs == {}

    args, kwargs = ParallelJob.normalize_params((2, 3, {'some_param': True}))
    assert args == (2, 3)
    assert kwargs == {'some_param': True}


def test_simple_params():
    expected = [
        ParallelJob(mocked_fn, args=(2, )),
        ParallelJob(mocked_fn, args=(3, )),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [2, 3])
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=(2, )),
        ParallelJob(mocked_fn, name='b', args=(3, )),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': 2,
        'b': 3
    })
    assert args == expected


def test_multiple_params():
    expected = [
        ParallelJob(mocked_fn, args=(2, 'R')),
        ParallelJob(mocked_fn, args=(3, 'S')),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [
        (2, 'R'),
        (3, 'S')])
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=(2, 'R')),
        ParallelJob(mocked_fn, name='b', args=(3, 'S')),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': (2, 'R'),
        'b': (3, 'S'),
    })
    assert args == expected


def test_named_params():
    expected = [
        ParallelJob(mocked_fn, args=(2, 'R'), kwargs={'some_param': True}),
        ParallelJob(mocked_fn, args=(3, 'S'), kwargs={'some_param': False}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [
        (2, 'R', {'some_param': True}),
        (3, 'S', {'some_param': False})
    ])
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=(2, 'R'), kwargs={'some_param': True}),
        ParallelJob(mocked_fn, name='b', args=(3, 'S'), kwargs={'some_param': False}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': (2, 'R', {'some_param': True}),
        'b': (3, 'S', {'some_param': False}),
    })
    assert args == expected


def test_parallel_args():
    expected = [
        ParallelJob(mocked_fn, args=(2, 'R'), kwargs={'some_param': True}),
        ParallelJob(mocked_fn, args=(3, 'S'), kwargs={'some_param': False}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [
        ParallelArg(2, 'R', some_param=True),
        ParallelArg(3, 'S', some_param=False)
    ])
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=(2, 'R'), kwargs={'some_param': True}),
        ParallelJob(mocked_fn, name='b', args=(3, 'S'), kwargs={'some_param': False}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': (2, 'R', {'some_param': True}),
        'b': (3, 'S', {'some_param': False}),
    })
    assert args == expected


def test_extra_params():
    expected = [
        ParallelJob(mocked_fn, args=(2, 'R'), kwargs={'some_param': True}),
        ParallelJob(mocked_fn, args=(3, 'S'), kwargs={'some_param': True}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [
        (2, 'R'),
        (3, 'S')
    ], extras={'some_param': True})
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=(2, 'R'), kwargs={'some_param': True}),
        ParallelJob(mocked_fn, name='b', args=(3, 'S'), kwargs={'some_param': True}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': (2, 'R'),
        'b': (3, 'S'),
    }, extras={'some_param': True})
    assert args == expected


def test_named_and_extra_params():
    expected = [
        ParallelJob(mocked_fn, args=(2, 'R'), kwargs={'some_param': True, 'other_param': 10}),
        ParallelJob(mocked_fn, args=(3, 'S'), kwargs={'some_param': True}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [
        (2, 'R', {'other_param': 10}),
        (3, 'S'),
    ], extras={'some_param': True})
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=(2, 'R'), kwargs={'some_param': True, 'other_param': 10}),
        ParallelJob(mocked_fn, name='b', args=(3, 'S'), kwargs={'some_param': True}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': (2, 'R', {'other_param': 10}),
        'b': (3, 'S'),
    }, extras={'some_param': True})
    assert args == expected


def test_named_and_extra_overrides_params():
    expected = [
        ParallelJob(mocked_fn, args=(2, 'R'), kwargs={'some_param': False}),
        ParallelJob(mocked_fn, args=(3, 'S'), kwargs={'some_param': True}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [
        (2, 'R', {'some_param': False}),
        (3, 'S'),
    ], extras={'some_param': True})
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=(2, 'R'), kwargs={'some_param': False}),
        ParallelJob(mocked_fn, name='b', args=(3, 'S'), kwargs={'some_param': True}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': (2, 'R', {'some_param': False}),
        'b': (3, 'S'),
    }, extras={'some_param': True})
    assert args == expected


def test_unpack_params_false():
    expected = [
        ParallelJob(mocked_fn, args=((2, 'R'), )),
        ParallelJob(mocked_fn, args=((3, 'S'), )),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [
        (2, 'R'),
        (3, 'S')], unpack_arguments=False)
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=((2, 'R'), )),
        ParallelJob(mocked_fn, name='b', args=((3, 'S'), )),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': (2, 'R'),
        'b': (3, 'S'),
    }, unpack_arguments=False)
    assert args == expected


def test_unpack_params_false_with_extras():
    expected = [
        ParallelJob(mocked_fn, args=((2, 'R'), ), kwargs={'some_param': True}),
        ParallelJob(mocked_fn, args=((3, 'S'), ), kwargs={'some_param': True}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [
        (2, 'R'),
        (3, 'S')
    ], extras={'some_param': True}, unpack_arguments=False)
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=((2, 'R'), ), kwargs={'some_param': True}),
        ParallelJob(mocked_fn, name='b', args=((3, 'S'), ), kwargs={'some_param': True}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': (2, 'R'),
        'b': (3, 'S'),
    }, extras={'some_param': True}, unpack_arguments=False)
    assert args == expected


def test_all_named_params_wrapped_in_sequence():
    expected = [
        ParallelJob(mocked_fn, args=tuple(), kwargs={'p1': 1, 'p2': 2}),
        ParallelJob(mocked_fn, args=tuple(), kwargs={'p1': 3, 'p2': 4}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [
        [{'p1': 1, 'p2': 2}],
        [{'p1': 3, 'p2': 4}],
    ])
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=tuple(), kwargs={'p1': 1, 'p2': 2}),
        ParallelJob(mocked_fn, name='b', args=tuple(), kwargs={'p1': 3, 'p2': 4}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': [{'p1': 1, 'p2': 2}],
        'b': [{'p1': 3, 'p2': 4}],
    })
    assert args == expected


def test_all_named_params_without_sequence():
    expected = [
        ParallelJob(mocked_fn, args=tuple(), kwargs={'p1': 1, 'p2': 2}),
        ParallelJob(mocked_fn, args=tuple(), kwargs={'p1': 3, 'p2': 4}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [
        {'p1': 1, 'p2': 2},
        {'p1': 3, 'p2': 4},
    ])
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=tuple(), kwargs={'p1': 1, 'p2': 2}),
        ParallelJob(mocked_fn, name='b', args=tuple(), kwargs={'p1': 3, 'p2': 4}),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': {'p1': 1, 'p2': 2},
        'b': {'p1': 3, 'p2': 4},
    })
    assert args == expected


def test_all_named_params_without_sequence_unpack_args_false():
    expected = [
        ParallelJob(mocked_fn, args=({'p1': 1, 'p2': 2}, )),
        ParallelJob(mocked_fn, args=({'p1': 3, 'p2': 4}, )),
    ]
    args = ParallelJob.build_from_params(mocked_fn, [
        {'p1': 1, 'p2': 2},
        {'p1': 3, 'p2': 4},
    ], unpack_arguments=False)
    assert args == expected

    expected = [
        ParallelJob(mocked_fn, name='a', args=({'p1': 1, 'p2': 2}, )),
        ParallelJob(mocked_fn, name='b', args=({'p1': 3, 'p2': 4}, )),
    ]
    args = ParallelJob.build_from_params(mocked_fn, {
        'a': {'p1': 1, 'p2': 2},
        'b': {'p1': 3, 'p2': 4},
    }, unpack_arguments=False)
    assert args == expected
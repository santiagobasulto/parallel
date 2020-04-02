import time

import parallel


def process_records_simple(records):
    return [r ** 2 for r in records]


def process_records_simple_sleep(records):
    if records == ['a', 'b']:
        # make it slower on purpose
        time.sleep(.2)
    return [c.upper() for c in records]


def process_records_extras(records, double=False, square=False):
    if not any([double, square]):
        raise ValueError('At least one needed')
    if square:
        return [r ** 2 for r in records]
    else:
        return [r * 2 for r in records]

def test_split_simple():
    records = [1, 2, 3, 4]
    results = parallel.split(records, process_records_simple, workers=2)
    assert results == [1, 4, 9, 16]

    records = [1, 2, 3, 4, 5]
    results = parallel.split(records, process_records_simple, workers=2)
    assert results == [1, 4, 9, 16, 25]


def test_split_simple_thread_process():
    records = [1, 2, 3, 4]
    results = parallel.thread.split(records, process_records_simple, workers=2)
    assert results == [1, 4, 9, 16]

    records = [1, 2, 3, 4, 5]
    results = parallel.process.split(records, process_records_simple, workers=2)
    assert results == [1, 4, 9, 16, 25]


def test_split_simple_verify_order():
    records = ['a', 'b', 'c', 'd']
    results = parallel.split(records, process_records_simple_sleep, workers=2)
    assert results == ['A', 'B', 'C', 'D']


def test_split_simple_extras():
    records = [1, 2, 3, 4]
    results = parallel.split(records, process_records_extras, workers=2, extras={'square': True})
    assert results == [1, 4, 9, 16]

    records = [1, 2, 3, 4, 5]
    results = parallel.split(records, process_records_extras, workers=2, extras={'double': True})
    assert results == [2, 4, 6, 8, 10]
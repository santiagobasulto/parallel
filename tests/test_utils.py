from parallel.utils import split_collection

def test_split_collection():
    records = [1, 2, 3, 4]
    parts = split_collection(records, 2)
    assert list(parts) == [[1, 2], [3, 4]]

    records = list(range(1, 13))
    parts = split_collection(records, 3)
    assert list(parts) == [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]

    records = [1, 2, 3, 4, 5]
    parts = split_collection(records, 2)
    assert list(parts) == [[1, 2, 3], [4, 5]]

    records = [1, 2, 3, 4, 5, 6, 7]
    parts = split_collection(records, 2)
    assert list(parts) == [[1, 2, 3, 4], [5, 6, 7]]

    records = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    parts = split_collection(records, 2)
    assert list(parts) == [[1, 2, 3, 4, 5], [6, 7, 8, 9]]

    records = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    parts = split_collection(records, 3)
    assert list(parts) == [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

    records = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    parts = split_collection(records, 2)
    assert list(parts) == [[1, 2, 3, 4, 5, 6], [7, 8, 9, 10, 11]]

    records = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    parts = split_collection(records, 3)
    assert list(parts) == [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11]]
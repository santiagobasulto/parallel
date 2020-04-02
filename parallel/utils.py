def split_collection(collection, chunks):
    if type(collection) not in (list, tuple):
        raise ValueError("This is an experimental feature, only list and tuples are supported.")

    is_odd = (len(collection) % chunks) != 0
    k = int(len(collection) / chunks)
    if is_odd:
        k += 1
    for i in range(chunks):
        beginning = i * k
        end = k * (i + 1)
        # if is_odd and i == 0:
        #     end += 1
        yield collection[beginning: end]
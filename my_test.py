
class Dec:
    def __init__(self, fn, num=None, string=False):
        self.fn = fn
        self.num = num
        self.string = string

    def __call__(self, a, b):
        print(self.num)
        val = self.fn(a, b)
        if self.string:
            return f"({val})"
        return val


def dec(*args, **kwargs):
    if kwargs:
        def wrapped(fn):
            return Dec(fn, *args, *kwargs)
        return wrapped
    else:
        fn = args[0]
        return Dec(fn)


@dec
def add(a, b):
    return a + b


@dec(7, string=True)
def mul(a, b):
    return a * b



# print(f"Result: {add(2, 3)}")
print(f"Result: {mul(3, 4)}")
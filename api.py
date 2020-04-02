def map(params, timeout):
    return ParallelHelper(ThreadExecutor).map(params, timeout)

@contextmanager
def async_map(params, timeout):
    jobs = build_jobs(params)

    ex = Executor(jobs, timeout)
    ex.start()
    try:
        yield ex
    finally:
        ex.shutdown()

class Job:
    name: Optional = None
    params: ParallelArgs
    future: concurrent.futures.Future = None

Executor(jobs)

class Executor:
    __status = 'waiting|started|idle'
    def init(max_workers, result_class=SequentialResults, ...):
        pass

    def map(fn, params):
        pass

    def start(self):
        assert self.status == 'waiting'
        self.__executor = ThreadPoolExecutor(max_workers=self.max_workers)

        for job in self.jobs:
            f = self.__executor.submit(job.fn, *job.args, **job.kwargs)
            job.future = f

    def results(self):
        res = self.result_class()
        for job in self.jobs:
            res.add(job.name, job.result(self.timeout))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.shutdown()

class ParallelHelper:
    def __init__(self, ExecutorClass):
        pass


res = parallel.map(fn, [
    (2, 3),
    (4, 5)
])


with parallel.async_map([(2, 3), (3, 4)]) as ex:
    print(ex.results())


with parallel.ThreadExecutor() as ex:
    ex.submit(fn, )
    ex.par(fn).submit((2, 3))

from concurrent.futures import ThreadPoolExecutor

def add(a, b):
    raise ValueError('puto')
    return a + b

with ThreadPoolExecutor(max_workers=4) as ex:
    f = ex.submit(add, 2, 3)
    import ipdb; ipdb.set_trace()
    try:
        f.result()
    except ValueError:
        print('CAUGHT')




bookmarks = [... 1_000_000_000 bookmarks ...]
results = parallel.split(bookmarks, process_bookmark, extras={
    'video_mapping': (1,2)
}, workers=10, )


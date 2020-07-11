`parallel` makes the process of writing parallel code simple and enjoyable, bringing parallelism **closer to mainstream developers**. `parallel` is **NOT** a pipeline library (check [Dask](https://dask.org/) or
[Luigi](https://github.com/spotify/luigi) for that).

`parallel` is inspired (and perfectly summarized) by
[Scala's parallel collections](https://docs.scala-lang.org/overviews/parallel-collections/overview.html):

> An effort to facilitate parallel programming by
  **sparing users from low-level parallelization details**, meanwhile providing
  them with a familiar and simple high-level abstraction.

> The hope was, and still is, that implicit parallelism behind
  a high-level abstraction will bring **reliable parallel execution one step
  closer to the workflow of mainstream developers**.

Installation (keep on reading for examples):
```bash
$ pip install python-parallel
```

Here's a quick look at what you can achieve with `parallel`:

```python
def download_and_store(url):
    resp = requests.get(url)
    result = store_in_db(resp.json())
    return result

urls = [
    'https://python.org',
    'https://python-requests.com',
    'https://rmotr.com'
]

# instant parallelism (Threads used by default)
results = parallel.map(
    download_and_store,  # the function to invoke
    urls,                # parameters to parallelize
    timeout=5,           # timeout per thread
    max_workers=4        # max threads
)

# results are ordered and are logically organized as you'd expect
results == [
    'https://python.org',
    'https://python-requests.com',
    'https://rmotr.com']
```

`parallel` is designed to transparently provide support for multithreading and multiprocessing. Switch between executors with just an argument:

```python
# Using multi-threading
results = parallel.thread.map(download_and_store, urls)

# Using multi-processing
results = parallel.process.map(download_and_store, urls)
```

If a certain function needs recurrent parallelization, you can choose to decorate it:

```python
@parallel.decorate
def download_and_store(url):
    # ...

results = download_and_store.map(  # parallelize the function directly
    urls,                          # parameters to parallelize
    timeout=5,                     # timeout per thread
    max_workers=4                  # max threads
)
```

and the function can still be used normally:

```python
res = download_and_store('https://rmotr.com')
```

All the functionality of `parallel` is presented with a blocking version (default) and non-blocking one:

```python
def download_and_store(url):
    pass

urls = ['https://python', 'https://rmotr.com', '...']

# instant parallelism (Threads used by default)
with parallel.async_map(download_and_store, urls, timeout=5) as ex:
    # do something else while parallel processes
    values = db.read_data()
    # access results when needed (this might block)
    results = ex.results()

# resources are cleaned up when the context manager exits
```

!!! note "Blocking vs Non-blocking"
    Please don't confuse non-blocking with _asynchronous_ execution (provided by the `async` module).
    Non blocking in this context means that your threads (or processes) are executed in the background,
    and you can keep working in the main thread meanwhile.

### Next steps

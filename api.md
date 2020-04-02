## High Level methods

#### `map`

Simple list parameter:
```python
results = parallel.map(download_and_store, [
    'https://python.org', 'https://github.com',
])
results.succeeded == ['https://python.org', ...]
```

Advanced dictionary parameters
```python
results = parallel.map(download_and_store, {
    'python': 'https://python.org',
    'rmotr': 'https://rmotr.com'
})
results['rmotr']
results['python']
```

Multiple parameters:

```python
parallel.map
parallel.async_map
parallel.async_spawn
parallel.par
parallel.async_par

# Utils
parallel.future

# Models
parallel.Result
parallel.ResultContainer
```

Test order:
* Single param
* Multi param
* Named params
* Extras
* Unpack Arguments

* Future
* Arg



Docs:

* Simple usage map
* Dict parameters
* Decorator form (warning threads)
* Named parameters
* Extras
* Unpack arguments




```python
def map(params, timeout)
    jobs = build_jobs(params)
    ex = Executor(jobs, timeout)
    return ex.results()


class Job:
    name: None
    params: ParallelArgs

Executor(jobs)
```
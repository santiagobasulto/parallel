`parallel`'s main goal is to make the process of parallelizing code transparent to us developers. For that, it offers 2 main functions (with their respective non-blocking versions): `parallel.map` and `parallel.par`.

### `parallel.map`

Used to spawn multiple executions of the same function with different parameters:

```python
def download_and_store(url, expected_code=200):
    resp = requests.get(url)
    if resp.status_code != expected_code:
        raise ValueError()
    result = store_in_db(resp.json())
    return result

urls = ['https://python.org', 'https://rmotr.com', '...']

results = parallel.map(download_and_store, urls)
python_result = results[0]
```

Multiple params can be passed just as sequences:

```python
urls = [
    ('https://python.org', 204),
    ('https://rmotr.com', 201),
]

results = parallel.map(download_and_store, urls)
```

And in dictionaries:

```python
urls = {
    'python': ('https://python.org', 204),
    'rmotr': ('https://rmotr.com', 201),
}

results = parallel.map(download_and_store, urls)
python_result = results['python']
```

(more about parameters below)

#### `parallel.par`

Used when you need to run different functions in parallel:

```python
def get_price_bitcoin(exchange):
    pass

def get_price_ether(exchange):
    pass

def get_price_ripple(exchange):
    pass

def index(request):
    prices = parallel.par({
        'btc': (get_price_bitcoin, 'bitstamp')
        'eth': parallel.future(get_price_ether, exchange='bitfinex'),
        'xrp': parallel.future(get_price_ripple, 'bitstamp'),
    })

    return render_template('index.html', context={
        'prices': prices
    })
```

We'll keep using these same examples to explore all the features of `parallel`. For most parts, these will work in the same way for all `map`, `async_map`, `par` and `async_par`.

### Choosing an executor

`parallel` can transparently provide multithreading or multiprocess execution for your code.

```python
# Multithreading:
results = parallel.thread.map(download_and_store, urls)   # Attribute
results = parallel.map(download_and_store, urls,
                       executor=parallel.THREAD_EXECUTOR) # Parameter

# Multiprocessing:
results = parallel.process.map(download_and_store, urls)   # Attribute
results = parallel.map(download_and_store, urls,
                       executor=parallel.PROCESS_EXECUTOR) # Parameter
```

Parameters for executors can be passed directly (similar to `concurrent.futures`):

```python
results = parallel.map(
    download_and_store, urls,
    executor=parallel.THREAD_EXECUTOR,
    timeout=10,
    max_workers=5)
```

### Decorating functions

If you rely on parallel tasks in a constant basis, you can choose to decorate the function to make it easier for later:

```python
@parallel.decorate
def download_and_store(url):
    pass

urls = ['https://python.org', 'https://rmotr.com', '...']

results = download_and_store.map(urls)  # Use the function as a handler
```

Executors and parameters all work in the same way:

```python
results = download_and_store.thread.map(urls, timeout=10, max_workers=5)

results = download_and_store.process.map(urls, timeout=10, max_workers=5)
```

For `parallel.par`, the advantage of decorating functions is the easy passage of parameters:

```python
@parallel.decorate
def get_price_bitcoin(exchange):
    pass

@parallel.decorate
def get_price_ether(exchange):
    pass

prices = parallel.par({
    'btc': get_price_bitcoin.future('bitstamp'),
    'eth': get_price_ether.future(exchange='bitfinex'),
})
```

Any decorated function can still be used normally, the decorator only adds the `parallel` attributes:

```python
result = download_and_store('https://python.org')
btc = get_price_bitcoin('bitstamp')
```

## Parameters for ease of mind

One of the biggest limitations with `concurrent.futures` is the passage of parameters. `parallel` resolves all those issues and even incorporates some good ideas to simplify your work:

### Sequences or dictionaries

Do you prefer to see results in a sequential manner, or in a _named_ one? Either can be used:

```python
def download_and_store(url):
    pass

# Sequential:
results = parallel.map(download_and_store, [
    'https://python.org',
    'https://rmotr.com'
])
results == ['python.org results', 'rmotr.com results']

# Named (with a dictionary):
results = parallel.map(download_and_store, {
    'python': 'https://python.org',
    'rmotr': 'https://rmotr.com'
})
results == {
    'python': 'python.org results',
    'rmotr': 'rmotr.com results'
}
```

### Named and optional parameters

When functions get more advanced, or invocations vary dynamically, `concurrent.futures` is more limited. `parallel` works with all the use cases that you might encounter. Consider the following function as an example:

```python
def download_and_store(url, content_type, db_table='websites', log_level='info', options=None):
    # ... some code ...
    pass
```

##### Passing multiple arguments

In its simplest form, passing multiple ordered arguments works just by passing the parameters in a sequence:

```python
results = parallel.map(download_and_store, [
    ('https://python.org', 'json'),  # download_and_store('https://python.org', 'json')
    ('https://rmotr.com', 'xml')     # download_and_store('https://rmotr.com', 'xml')
])
```

##### Passing named parameters

To pass named parameters, just use a dictionary inside the parameter's sequence:

```python
results = parallel.map(download_and_store, [
    # download_and_store('https://python.org', 'json', db_table='webpages')
    ('https://python.org', 'json', {'db_table': 'webpages'}),

    # download_and_store('https://rmotr.com', 'xml', db_table='debug')
    ('https://rmotr.com', 'xml', {'log_level': 'debug'})
])
```

### Extras, to avoid repeating yourself

Instead of passing the same named parameter over and over again, pass it as an _extra_:

```python
results = parallel.map(download_and_store, [
    ('https://python.org', 'json'),
    ('https://rmotr.com', 'xml')
], extras={
    'db_table': 'temporary_webpages'
})
```

In this example, all the URLs will be stored in `temporary_webpages`.

### Advanced use cases

When faced with very extreme scenarios of function invocation, you can resort back to `parallel.arg`. For example, if your function receives a dictionary as parameter, `parallel` by default will interpret it as a named parameter. To avoid that, just use `parallel.arg`:


```python
results = parallel.map(download_and_store, {
    'python': parallel.arg('https://python.org', options={'Content-Type': 'application/json'}),
    'rmotr': 'https://rmotr.com'
})
```

For `parallel.par`, you can use `parallel.future`:

```python
prices = parallel.par({
    'btc': parallel.future(get_price_bitcoin, exchange='bitstamp')
    'eth': parallel.future(get_price_ether, 'bitfinex',
                           options={'Content-Type': 'application/json'}),
})
```

## Error Handling

By default, if a function raises an exception, `parallel` will propagate it, this is the expected behavior. But `parallel` also includes error handling capabilities. By using the `silent` argument, you can _silence_ exceptions and access the details of the failed tasks afterwards (including the exception raised).

```python
def add(x, y):
    pass

results = parallel.map(add, [
    (2, 3),
    ('a', 5),  # will fail
    ('hello ', 'world')
], silent=True)

print(results.failures)
# True (there are failed tasks)

r1, r2, r3 = results
assert r1 == 5
assert r3 == 'hello world'

assert r2 == parallel.FailedTask(
    params=('a', 5),
    ex=TypeError('unsupported operand type(s) for +: 'int' and 'str'')
)

# Read below for more info on `replace_failed`
assert results.replace_failed(None) == [5, None, 'hello world']
```

The `parallel.FailedTask` model includes information of the failed tasks, including the arguments (both sequential and named) passed.

What `map`, `par` and other methods return is an instance of the `parallel.BaseResult` class. These results include a few convenient methods:

* `results.failures` (_Boolean_): `True` if there were any tasks that failed with an exception.
* `results.failed` (_List_): A list of all the failed tasks.
* `results.succeeded` (_List_): A list of all the successful tasks.
* `results.replace_failed`: Receives a value to use for replacement of all the failed tasks.
# parallel

> Bringing _parallelism_ closer to humans ✨


**Important**: `parallel` is in an early stage. All questions and suggestions are welcome. Please [submit an issue](https://github.com/santiagobasulto/parallel/issues).

### Getting started

Install:

```bash
$ pip install python-parallel
```

_Parallelize_:

```python
@parallel.decorate
def download_and_store(url):
    resp = requests.get(url)
    result = store_in_db(resp.json())
    return result

results = download_and_store.map([
    'https://python.org',
    'https://python-requests.com',
    'https://rmotr.com'
], timeout=5, max_workers=4)
```

There are more features to `parallel`:
* Advanced argument passing (named arguments, extras).
* Failed tasks and a silent mode.
* An Async API (with context managers).
* Automatic retries
* Choose between multithreading and multiprocessing with a simple attribute `thread` & `process`.

Check the docs at [https://python-parallel.readthedocs.io/en/latest/](https://python-parallel.readthedocs.io/en/latest/).

## Quick Docs

`parallel` simplifies the process of _parallelizing_ tasks in your python code. Sometimes, you have a function that you want to invoke multiple times in parallel with different arguments (as the example above).

In some other occasions, you want to execute multiple functions in parallel. Example:

```python
@parallel.decorated
def get_price_bitstamp(crypto):
    pass

@parallel.decorated
def get_price_bitfinex(crypto):
    pass

@parallel.decorated
def get_price_coinbase(crypto):
    pass

prices = parallel.par({
    'stamp': get_price_bitstamp.future(crypto='BTC'),
    'finex': get_price_bitfinex.future(crypto='BTC'),
    'base': get_price_coinbase.future(crypto='BTC'),
})

# prices is a dict-like structure
print("Price of Bitstamp: {}".format(prices['stamp']))
print("Price of Coinbase: {}".format(prices['base']))
```

## Contributing

Run tests

```bash
$ pip install -r dev-requirements.txt
$ py.test -n 4 tests/
```

Write & build docs:
```bash
$ python docs/live_docs.py
```
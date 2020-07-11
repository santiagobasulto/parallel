<h1 align="center">
  <br>
  <a href="https://santiagobasulto.github.io/parallel/"><img src="https://user-images.githubusercontent.com/872296/76652584-44ce4280-653d-11ea-8b8c-5e33939f1762.png" alt="Python Parallel" width="200"></a>
  <br>
  Parallel
  <br>
</h1>

<h4 align="center">Instant multi-thread/multi-process parallelism for Python ✨</h4>

<p align="center">
  <a href="https://travis-ci.org/github/santiagobasulto/parallel">
    <img src="https://img.shields.io/travis/santiagobasulto/parallel" alt="Travis Builds" />
  </a>
</p>

**Important**: `parallel` is in an early stage. All questions and suggestions are welcome. Check the section [#Contributing](#Contributing) below or [submit an issue](https://github.com/santiagobasulto/parallel/issues).

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

Check the docs at [https://santiagobasulto.github.io/parallel/](https://santiagobasulto.github.io/parallel/).

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

Parallel is in very early stage. It's an effort to conceptualize the requirements that I've had throughout the years when writing parallel code. Simplicity has been my main motivation when writing the library, keeping it simple for the user to use and readable once the code has been written.

Your comments and suggestions are greatly appreciated. If you think that any use case has been missing, or we should improve the API in any way, please [submit an issue](https://github.com/santiagobasulto/parallel/issues).

Parallel uses poetry:

```bash
$ poetry install
$ poetry shell
$ make test
```

Documentation is written using MKDocs, the following command builds them live:

```bash
$ mkdocs serve
```

# parallel

> Bringing _parallelism_ closer to humans ✨


**Important**: `parallel` is in an early stage. All questions and suggestions are welcome. Please [submit an issue](https://github.com/santiagobasulto/parallel/issues).

### Getting started

Install:

```bash
$ pip install python-parallel
```

_Parallalize_:

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

Check the docs at [https://python-parallel.readthedocs.io/en/latest/](https://python-parallel.readthedocs.io/en/latest/).

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
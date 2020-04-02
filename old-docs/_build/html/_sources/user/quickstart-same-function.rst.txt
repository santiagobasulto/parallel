################################################################
Quickstart: Parallelize a function with multiple executions
################################################################

**Important:** Most of the examples in this document are written using the
**Blocking API** and **decorated** function style. At the end of this doc
there are examples of both, **non-decorated** functions and the
**Non Blocking API**.


The basics
------------

In its simpler form, parallel lets you fire up multiple executions of the same function at the same time (in parallel), with the ``map`` function::

    @parallel.decorate
    def download_and_store(url):
        resp = requests.get(url)
        result = store_in_db(resp.json())
        return result

    urls = [
        'https://python.org',
        'https://python-requests.com',
        'https://rmotr.com'
    ]
    results = download_and_store.map(urls)

    # Also includes a summary of the execution
    results.succeeded == ['https://python.org', ...]

From this example, ``results`` is a list-like structure containing the
results of the function executions, in the same order as they were passed.
``results`` is a rich sequence which also contains a summary of the
executions and, when run in silent mode, the errors that might arise
(check the section *Errors & Silent Mode* **TODO**).

Running the original function verbatim
--------------------------------------

Parallel will not modify the original function, which can be invoked normally::

    result = download_and_store('https://python.org')

Named executions (using dictionaries)
--------------------------------------

The name chosen for ``map`` is just a convention similar to ``concurrent.futures``. But ``map`` is not limited to sequences, it also accepts dictionaries, which allows accessing the results "by name"::

    results = download_and_store.map({
        'python': 'https://python.org',
        'rmotr': 'https://rmotr.com'
    })

    results['rmotr'] # the results of `rmotr` execution
    results['python'] # the results of `python` execution


Passing multiple parameters
----------------------------

Parallel accounts for the usecases of functions receiving multiple parameters
(as well as named and optional arguments, described below).
When using ``map`` with a sequence, Parallel will *"expand"* the individual
objects in the sequence as they were multiple parameters::

    @parallel.decorate
    def download_and_store(url, outfile):
        pass

    results = download_and_store.map([
        # The params will be expanded in order:
        # (url, outfile)
        ('https://python.org', 'python.html'),
        ('https://rmotr.com', 'rmotr.html'),
    ])

But, what if your function actually expects a sequence?
You could choose to *"nest"* the parameters, but that doesn't read well,
so Parallel includes the option ``unpack_arguments`` (by default being ``True``)::

      @parallel.decorate
      def download_and_store(params):
          # the function receives a tuple
          url, outfile = params
          pass

      results = download_and_store.map([
          # params passed as tuples
          ('https://python.org', 'python.html'),
          ('https://rmotr.com', 'rmotr.html'),
      ], unpack_arguments=False)

Passing Named Arguments
------------------------

Simply pass named argument to your functions as dictionaries::

    @parallel.decorate
    def download_and_store(url, outfile=None, buffer=4):
        pass

    results = download_and_store.map([
        ('https://python.org', {'outfile': 'python.html'}),
        ('https://rmotr.com', {'buffer': 8}),
    ])

If you prefer to be extra explicit and pass ALL the parameters as
named arguments, you can use a dictionary instead of a sequence (tuple)::

      @parallel.decorate
      def download_and_store(url, outfile):
          pass

      results = download_and_store.map([
          {'url': 'https://python.org', 'outfile': 'python.html'},
          {'url': 'https://rmotr.com', 'outfile': 'rmotr.html'},
      ])


Extras: simplify repeated arguments
------------------------------------

If there are arguments repeated for every execution, you can avoid the
repetition by placing it in the ``extras`` section. Check the following example
with and without ``extras``::

    @parallel.decorate
    def download_and_store(url, outfile, pretty, indent=2):
        pass

    # WithOUT extras:
    results = download_and_store.map([
        ('https://python.org', {
            'outfile': 'python.html',
            'pretty': True,  # repeated!
            'indent': 4}),  # repeated!
        ('https://rmotr.com', {
            'outfile': 'rmotr.html',
            'pretty': True,  # repeated!
            'indent': 4}),  # repeated!
    ])

    # With extras:
    results = download_and_store.map([
        ('https://python.org', {'outfile': 'python.html'}),
        ('https://rmotr.com', {'outfile': 'rmotr.html'}),
    ], extras={
        'pretty': True,
        'indent': 4
    })

In the example above, ``pretty=True`` and ``indent=4`` will be passed to
every function execution. **Important**: this only works with named arguments.

Advanced argument passing
---------------------------

All the features shown above (sequences for parameters, dictionaries for
named parameters and ``extras``) are backed by a custom ``parallel.arg``
object that contains the parameters of your functions.

**Important**. The usage of ``parallel.arg`` is discouraged as the API
could change. If you find yourself using it, please report it as an issue
so we can see what shortcoming we should fix.

Still, here's an example of it::

  @parallel.decorate
  def download_and_store(url, outfile, pretty, indent=2):
      pass

  results = download_and_store.map([
      parallel.arg(
          'https://python.org', outfile='python.html', pretty=False),
      parallel.arg(
          'https://rmotr.com', outfile='rmotr.html', indent=4),
  ])

Other execution options
-----------------------

You can control other options like ``timeout`` and ``max_workers`` when
invoking ``map``::

    @parallel.decorate
    def download_and_store(url):
        resp = requests.get(url)
        result = store_in_db(resp.json())
        return result

    urls = [
        'https://python.org',
        'https://python-requests.com',
        'https://rmotr.com'
    ]
    results = download_and_store.map(urls, timeout=5, max_workers=8)


Error handling & the Silent mode
---------------------------------

By default, if an exception is raised in any of the parallel executions,
the whole parallel call will fail and propagate that exception.
There's also a *silent* mode that can be activated, that will catch all
the exceptions raised and return the tasks that didn't fail::

    @parallel.decorate
    def download_and_store(url, outfile):
        pass

    results = download_and_store.map([
        ('https://python.org', 'python.html'),
        ('http://non-existent-website.com', 'error.html'),  # will fail
        ('https://rmotr.com', 'rmotr.html'),
    ], silent=True)

    # Check if something failed
    results.failures == True

    # The tasks that didn't fail are available
    results.succeeded == ['https://python.org', 'https://rmotr.com']

    # Failed tasks are accessible
    failed_task = results.failed[0]
    failed_task == parallel.FailedTask(
        params=('http://non-existent-website.com', 'error.html'),
        ex=requests.ConnectionError('<redacted> Error message')
    )

Non Decorated Functions
------------------------

The examples shown so far used the *decorated* function style. If you can't
(or don't want) decorate the function, you can still use parallel in its
full extent.

To support this, we include a high-level ``parallel.map`` function that
acts as the regular ``map`` operation of decorated functions::

    def download_and_store(url):
        resp = requests.get(url)
        result = store_in_db(resp.json())
        return result

    results = parallel.map(download_and_store, [
        'https://python.org',
        'https://python-requests.com',
        'https://rmotr.com'
    ])

    results.succeeded == ['https://python.org', ...]

The first parameter of ``parallel.map`` is the function to parallalize; the
rest of the arguments work in the same way. Let's see another example with all
the features working at the same time::

    def download_and_store(url, outfile=None, buffer=4):
        pass

    results = parallel.map(download_and_store, [
        ('https://python.org', {'outfile': 'python.html'}),
        ('https://rmotr.com', {'buffer': 8}),
    ], extras={
        'buffer': 4
    }, timeout=5, max_workers=8)

A quick note on Python decorators
---------------------------------

Keep in mind that in Python, decorators are just functions applied to
other functions. Which means that the decoration limitation
could be easily circumvented with::

    import parallel

    # Not decorated function
    def sum(a, b):
        return a + b

    # Quick hack to simulate decorator.
    parallel_sum = paralllel.decorated(sum)

    parallel_sum.map([
        (2, 3),
        (9, 8),
        (5, 5)
    ])

The Non-Blocking (async) API
-----------------------------

Parallel also includes a *Non Blocking API*, which starts the execution of
the tasks and returns immediately. Here's an example::

    @parallel.decorate
    def download_and_store(url):
        resp = requests.get(url)
        result = store_in_db(resp.json())
        return result

    urls = [
        'https://python.org',
        'https://python-requests.com',
        'https://rmotr.com'
    ]
    with download_and_store.async_map(urls, max_workers=8) as ex:
        # do something here
        results = ex.results(timeout=4)

This also works for *non decorated* functions::

    def download_and_store(url):
        resp = requests.get(url)
        result = store_in_db(resp.json())
        return result

    urls = [
        'https://python.org',
        'https://python-requests.com',
        'https://rmotr.com'
    ]
    with parallel.async_map(download_and_store, urls, max_workers=8) as ex:
        # do something here
        results = ex.results(timeout=4)
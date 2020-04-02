##################################################
Quickstart: Run multiple functions in parallel
##################################################

**Important:** Most of the examples in this document are written using the
**Blocking API** and **decorated** function style. At the end of this doc
there are examples of both, **non-decorated** functions and the
**Non Blocking API**.


The basics
------------

"Multiple functions" refers to the use case of invoking multiple **different**
functions in parallel. Here's an example of a simple `Django <https://www.djangoproject.com/>`_
view that displays the price of Bitcoin from multiple exchanges::

    @parallel.decorated
    def get_price_bitstamp(crypto):
        pass

    @parallel.decorated
    def get_price_bitfinex(crypto):
        pass

    @parallel.decorated
    def get_price_coinbase(crypto):
        pass

    def index(request):
        prices = parallel.par({
            'stamp': get_price_bitstamp.future(crypto='BTC'),
            'finex': get_price_bitfinex.future(crypto='BTC'),
            'base': get_price_coinbase.future(crypto='BTC'),
        })

        return render_template('index.html', context={
            'prices': prices
        })

Template code::

    <ul>
        <li>Bitstamp: {{ prices.stamp }}</li>
        <li>BitFinex: {{ prices.finex }}</li>
        <li>Coinbase: {{ prices.base }}</li>
    </ul>

Running the original function verbatim
--------------------------------------

Parallel will not modify the original function, which can be invoked normally::

    stamp_price = get_price_bitstamp(crypto='BTC')

Sequential or named executions
----------------------------------

``parallel.par`` accepts both a dictionary or a sequence (a list) of functions
to execute in parallel. Here's a sequential example::

    prices = parallel.par([
        get_price_bitstamp.future(crypto='BTC'),
        get_price_bitfinex.future(crypto='BTC'),
        get_price_coinbase.future(crypto='BTC'),
    ])

    prices == [7800.20, 7821.89, 7789.32]

Passing parameters
----------------------

The recommended way to pass functions you want to run to ``parallel.par`` is
with the helper ``<function>.future`` which also works works with not decorated
functions. Let's see a complete example for both cases::

    @parallel.decorated
    def get_price_bitstamp(crypto, period='5m'):
        pass

    # Not Decorated!
    def get_price_bitfinex(crypto, period='5m'):
        pass

    parallel.par([
        get_price_bitstamp.future('BTC', period='10m'),
        # not decorated function:
        parallel.future(get_price_bitfinex, 'BTC', period='10m')
    ])

Alternatively, you can pass the functions as tuples in the form:
``([function or callable], arg1, arg2, ..., {'kwarg1': val, ...})``.
Let's see an example::

    parallel.par([
        (get_price_bitstamp, 'BTC', {'period': '10m'}),
        (get_price_bitfinex, 'BTC', {'period': '10m'}),
    ])

Extras: simplify repeated arguments
------------------------------------

If there are arguments repeated for every execution, you can avoid the
repetition by placing it in the ``extras`` section. This is how it looks
refactoring the code from the previous example::

    parallel.par([
        (get_price_bitstamp, 'BTC'),
        (get_price_bitfinex, 'BTC'),
    ], extras={
        'period': '10m'
    })

Other execution options
-----------------------

You can control other options like ``timeout`` and ``max_workers``::

    parallel.par([
        get_price_bitstamp.future('BTC'),
        get_price_bitfinex.future('BTC'),
    ], timeout=10, max_workers=4)

Error handling & the Silent mode
---------------------------------

By default, if an exception is raised in any of the parallel executions,
the whole parallel call will fail and propagate that exception.
There's also a *silent* mode that can be activated, that will catch all
the exceptions raised and return the tasks that didn't fail::


    prices = parallel.par({
        'stamp': get_price_bitstamp.future('BTC'),
        'finex': get_price_bitfinex.future('BTC', period='5m'),  # FAILS!
        'base': get_price_coinbase.future('BTC'),
    }, silent=True)

    # Check if something failed
    prices.failures == True

    # The tasks that didn't fail are available
    prices.succeeded == {
        'stamp': 7800.20,
        'base': 7789.32
    }

    results.failed == {
        'finex': parallel.FailedTask(
            params=('BTC',),
            kwargs={'period': '5m'}
            ex=requests.ConnectionError('<redacted> Error message')
        )
    }

The Non-Blocking (async) API
-----------------------------

Parallel also includes a *Non Blocking API*, which starts the execution of
the tasks and returns immediately. Here's an example::

    functions = {
        'stamp': get_price_bitstamp.future('BTC'),
        'finex': get_price_bitfinex.future('BTC'),
        'base': get_price_coinbase.future('BTC'),
    }
    with parallel.async_par(functions, max_workers=5) as ex:
        # do something here
        prices = ex.results(timeout=4)

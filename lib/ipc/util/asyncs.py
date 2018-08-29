import logging
import sys
import traceback
from functools import partial, wraps
from queue import Queue, Empty
from pprint import pformat

import asyncio
from twisted.internet import reactor
from twisted.internet.defer import (
    inlineCallbacks, Deferred, returnValue, maybeDeferred, DeferredList)
from twisted.internet.threads import deferToThread
from twisted.python.failure import Failure

from .colls import walk, set_path, byteify

time = reactor.seconds


class TimeoutError(Exception): pass


# FIXME: Don't know whether there is idiomatic way to use it
class NeverRaised(Exception): pass


async def _wait(func, interval=0.1, timeout=30, check=lambda _: True,
          ignore_exc=NeverRaised, msg=None, raise_on_timeout=True):

    max_time = time() + timeout

    while time() <= max_time:
        try:
            result = await asyncio.coroutine(func)()
        except ignore_exc:
            pass
        else:
            if check(result):
                return result
        await asyncio.sleep(interval)
    else:
        if raise_on_timeout:
            raise TimeoutError(msg)

class LoopingCall:
    def __init__(self, func):
        self.func = func

    async def _start(self, interval):
        while True:
            try:
                await asyncio.coroutine(self.func)()
            except Exception as e:
                logging.exception(e)
            await asyncio.sleep(interval)

    def start(self, interval):
        self.fut = asyncio.ensure_future(self._start(interval))

    def stop(self):
        self.fut.cancel()


@inlineCallbacks
def collect(f, interval=0.1, timeout=None, count=None):
    assert timeout or count, 'neither timeout nor count passed'
    max_time = time() + (timeout or sys.maxsize)
    results = []

    while time() <= max_time and len(results) < count:
        result = yield f()
        results.append(result)
        yield sleep(interval)
    else:
        returnValue(results)


def recur(f, interval=0.1, timeout=30):
    return _wait(f, interval, timeout, lambda _: False, raise_on_timeout=False)


def wait_true(func, interval=0.1, timeout=30, ignore_exc=NeverRaised, msg=None):
    return _wait(func, interval, timeout, bool, ignore_exc, msg=msg)


def retry(func, exc, interval=0.1, timeout=30, msg=None):
    return _wait(func, interval, timeout, ignore_exc=exc, msg=msg)


# deferred extensions: ---------------------------------------------------------

def deferEvent(event, condition=None):
    d = Deferred()
    def callback(*args, **kwargs):
        if condition is None or condition(*args, **kwargs):
            event.__isub__(callback)
            d.callback((args, kwargs))
    event += callback
    return d


@inlineCallbacks
def gatherCollection(coll):
    """Replace nested deferreds with their values, inplace!"""
    deferreds = []
    for path, defr in walk(coll, lambda x: isinstance(x, Deferred)):
        defr.addCallback(partial(set_path, coll, path))
        deferreds.append(defr)
    yield DeferredList(deferreds)


def blockOnDeferred(d, timeout=None):
    """Never use this function in reactor's thread!"""
    q = Queue()
    reactor.callFromThread(d.addBoth, q.put)
    try:
        ret = q.get(timeout=timeout)
    except Empty:
        raise TimeoutError
    if isinstance(ret, Failure):
        ret.raiseException()
    else:
        return ret


def threaded(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        return deferToThread(f, *args, **kwargs)
    return wrapped


def blockingDeferredCall(f):  # fixme: rename to `blocking`
    @wraps(f)
    def wrapped(*args, **kwargs):
        return blockOnDeferred(maybeDeferred(f, *args, **kwargs))
    return wrapped


def run_main(f, *args, **kwargs):
    status = {'exit_code': 0}

    @inlineCallbacks
    def run():
        try:
            result = yield f(*args, **kwargs)
            logging.warning(pformat(byteify(result)))
        except Exception:
            logging.critical(traceback.format_exc())
            status['exit_code'] = 1

        # fixme: dirty hack to prevent reactor from logging post-mortal errors
        try:
            from twisted.python.log import newGlobalLogPublisher
            del newGlobalLogPublisher._observers[:]
        except Exception:
            pass

        reactor.stop()

    reactor.callLater(0, run)
    reactor.run()
    sys.exit(status['exit_code'])

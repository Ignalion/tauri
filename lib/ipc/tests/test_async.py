from functools import partial
from itertools import count

import pytest
from twisted.trial import unittest
from twisted.python import failure
from twisted.internet.defer import inlineCallbacks, returnValue

from testcore.util import async


class TestAsync(unittest.TestCase):

    def setUp(self):
        self.hasTimeCome = lambda new_time: async.time() >= new_time

        counter = count()
        self.raisesTwice = lambda value: 1/0 if next(counter) < 2 else value

        self.start_time = async.time()

    @inlineCallbacks
    def testSleep(self):
        yield async.sleep(0.1)
        self.assertAlmostEqual(async.time() - self.start_time, 0.1, 1)

    @inlineCallbacks
    def testWait(self):
        yield async.wait_true(partial(self.hasTimeCome, async.time() + 0.1))
        self.assertAlmostEqual(async.time() - self.start_time, 0.1, 1)

    @inlineCallbacks
    def testWaitTimeout(self):
        interval, timeout = 0.1, 0.2
        with self.assertRaises(async.TimeoutError):
            yield async.wait_true(
                partial(self.hasTimeCome, async.time() + 2), interval, timeout)
        self.assertAlmostEqual(async.time() - self.start_time, timeout, 0)

    @inlineCallbacks
    def testRepeat(self):
        interval = 0.1
        v = yield async.retry(
            partial(self.raisesTwice, 'x'), ZeroDivisionError, interval)
        self.assertEqual(v, 'x')
        self.assertAlmostEqual(async.time() - self.start_time, interval * 2, 1)

    @inlineCallbacks
    def testRepeatNotCaught(self):
        with self.assertRaises(ZeroDivisionError):
            yield async.retry(partial(self.raisesTwice, 'x'), RuntimeError)
        self.assertAlmostEqual(async.time() - self.start_time, 0, 1)

    @inlineCallbacks
    def testRepeatTimeout(self):
        interval, timeout = 0.1, 0.1
        with self.assertRaises(async.TimeoutError):
            yield async.retry(
                partial(self.raisesTwice, 'x'), ZeroDivisionError, interval, timeout)
        self.assertAlmostEqual(async.time() - self.start_time, timeout, 0)


@pytest.inlineCallbacks
def asleep(t):
    yield async.sleep(t)
    returnValue(t or t / t)


@pytest.fixture
def defercoll():
    return {
        1: asleep(0.1),
        2: {3: asleep(0.3), 4: [asleep(0.5), asleep(0.6)]}
    }


@pytest.inlineCallbacks
def test_gather_collection(defercoll):
    yield async.gatherCollection(defercoll)
    print(defercoll)
    assert defercoll == {1: 0.1, 2: {3: 0.3, 4: [0.5, 0.6]}}


@pytest.inlineCallbacks
def test_gather_collection_error(defercoll):
    defercoll[0] = asleep(0)
    yield async.gatherCollection(defercoll)
    assert isinstance(defercoll.pop(0).result, failure.Failure)
    assert defercoll == {1: 0.1, 2: {3: 0.3, 4: [0.5, 0.6]}}
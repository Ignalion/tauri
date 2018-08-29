from itertools import count
import asyncio
import time

import pytest

from ..util import asyncs


@pytest.fixture
def start_time():
    return time.time()


def check_time(tstmp):
    def _check():
        return time.time() >= tstmp
    return _check


def raises_twice(value):
    counter = count()
    def _func():
        return 1/0 if next(counter) < 2 else value
    return _func


@pytest.mark.asyncio
async def test_sleep(start_time):
    await asyncio.sleep(0.1)
    assert round(time.time() - start_time, 1) == 0.1


@pytest.mark.asyncio
async def test_wait(start_time):
    await asyncs.wait_true(check_time(start_time + 0.1))
    assert round(time.time() - start_time, 1) == 0.1


@pytest.mark.asyncio
async def test_wait_timeout(start_time):
    interval, timeout = 0.1, 0.2
    with pytest.raises(asyncs.TimeoutError):
        await asyncs.wait_true(check_time(start_time + 2), interval, timeout)
    assert round(time.time() - start_time, 1) == timeout


@pytest.mark.asyncio
async def test_repeat(start_time):
    interval = 0.1
    v = await asyncs.retry(raises_twice('x'), ZeroDivisionError, interval)
    assert v == 'x'
    assert round(time.time() - start_time, 1) == interval * 2


@pytest.mark.asyncio
async def test_repeat_not_caught(start_time):
    with pytest.raises(ZeroDivisionError):
        await asyncs.retry(raises_twice('x'), RuntimeError)
    assert round(time.time() - start_time, 1) == 0


@pytest.mark.asyncio
async def test_repeat_timeout(start_time):
    interval, timeout = 0.1, 0.1
    with pytest.raises(asyncs.TimeoutError):
        await asyncs.retry(raises_twice('x'), ZeroDivisionError, interval, timeout)
    assert round(time.time() - start_time, 1) == timeout


# FIXME: Have to clarify whether we need this in asyncio environment
# @pytest.inlineCallbacks
# def asleep(t):
#     yield asyncs.sleep(t)
#     returnValue(t or t / t)
#
#
# @pytest.fixture
# def defercoll():
#     return {
#         1: asleep(0.1),
#         2: {3: asleep(0.3), 4: [asleep(0.5), asleep(0.6)]}
#     }
#
#
# @pytest.inlineCallbacks
# def test_gather_collection(defercoll):
#     yield asyncs.gatherCollection(defercoll)
#     print(defercoll)
#     assert defercoll == {1: 0.1, 2: {3: 0.3, 4: [0.5, 0.6]}}
#
#
# @pytest.inlineCallbacks
# def test_gather_collection_error(defercoll):
#     defercoll[0] = asleep(0)
#     yield asyncs.gatherCollection(defercoll)
#     assert isinstance(defercoll.pop(0).result, failure.Failure)
#     assert defercoll == {1: 0.1, 2: {3: 0.3, 4: [0.5, 0.6]}}

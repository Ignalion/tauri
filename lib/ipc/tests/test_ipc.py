import asyncio
import _pickle
import logging
import traceback
from collections import namedtuple
from multiprocessing import Process
from time import time

import pytest

from lib.ipc import get_router, TimeoutError
from lib.ipc import router
from lib.ipc.proxy import apply
from lib.ipc.gateways import basegw
from lib.ipc.route import normalize
from lib.ipc.util import asyncs


from lib.ipc.gateways import pikagw
pikagw.MQ_HOST = '127.0.0.1'
EXCHANGE = 'ROOT'

LOG_FORMAT = '%(asctime)s - %(processName)s->%(filename)s:%(lineno)d-' \
             ' %(levelname)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT)
logging.getLogger().setLevel(logging.DEBUG)
router.HEARTBEAT_INTERVAL = 0.1
router.HEARTBEAT_ERROR = 0.6
basegw.GW_LOOP_INTERVAL = 0.01
log = logging.getLogger(__name__).info


class CommandMock(object):

    def __init__(self):
        self._prop = 0
        self._other = 0

    @property
    def other_property(self):
        print('get other property:', self._other)
        return self._other

    @other_property.setter
    def other_property(self, value):
        print('set other property:', value)
        self._other = value

    @property
    def some_property(self):
        print('get property:', self._prop)
        return self._prop

    @some_property.setter
    def some_property(self, value):
        print('set property:', value)
        self._prop = value

    def set_some_property(self, value):
        self.some_property = value

    def p_immediate(self, value):
        if self.some_property == value:
            return 'SUCCESS'
        else:
            raise router.NoResponse

    def p_error(self, value):
        if self.some_property == value:
            return 1/0
        else:
            raise router.NoResponse

    async def delayed(self, timeout, value):
        await asyncio.sleep(timeout)
        return value

    async def delayed_error(self, timeout):
        await asyncio.sleep(timeout)
        return 1/0

    def immediate(self, value):
        return value

    def immediate_error(self):
        return 1/0

    def get_self(self):
        return self

    def check_self(self, obj):
        print(obj)
        return obj is self

    def unpicklable(self):
        return namedtuple('x', ['y', 'z'])

    def stop(self):
        loop = asyncio.get_event_loop()
        loop.stop()
        loop.close()


def init_router(component, pid=None, exch=None, local=False):
    r = get_router(component, pid=pid, exchange=exch)
    r.handlers.mock = CommandMock()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(r.start())
    if local:
        return r
    else:
        async def sleep(t):
            r.stop()
            await asyncio.sleep(t)
            asyncio.ensure_future(r.start())
        r.handlers.sleep = sleep
        r.handlers.reactor = loop
        loop.run_forever()


async def stop_remote(r, route):
    try:
        await (~ r.proxy(route).mock.stop())
    except router.HeartbeatError:
        pass


@pytest.fixture(scope='session')
def remote_routers():
    Process(target=init_router, args=('remote', 0, EXCHANGE)).start()
    Process(target=init_router, args=('remote', 1, EXCHANGE)).start()


@pytest.yield_fixture(scope='session')
def pika_router(remote_routers, event_loop):
    logging.critical(event_loop)
    blockon = event_loop.run_until_complete
    r = init_router('local', exch=EXCHANGE, local=True)
    blockon(asyncs.wait_true(lambda: r.ready))
    remote0 = normalize(r, 'remote:0')
    remote1 = normalize(r, 'remote:1')
    blockon(r.wait(remote0, 100))
    blockon(r.wait(remote1, 100))
    log('proxy repr: %s', r.proxy(remote0).mock)
    yield r
    blockon(stop_remote(r, remote0))
    blockon(stop_remote(r, remote1))
    r.stop()


@pytest.fixture
def remote_mock(pika_router):
    return pika_router.proxy(normalize(pika_router, 'remote:0')).mock


@pytest.fixture
def group_mock(pika_router):
    return pika_router.proxy(normalize(pika_router, 'remote'), targets=2).mock


@pytest.yield_fixture(scope='session')
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop()
    logging.debug(loop)
    yield loop
    loop.close()


@pytest.fixture
def start_time():
    return time()


@pytest.mark.asyncio
async def test_immediate(remote_mock, start_time, event_loop):
    logging.critical(event_loop)
    result = await (~ remote_mock.immediate('<X>'))
    assert round(time() - start_time, 1) == 0.1  # FIXME Should be 0
    log('OK. immediate: %s', result)


@pytest.mark.asyncio
async def test_delayed(remote_mock, start_time):
    result = await (~ remote_mock.delayed(0.5, 42))
    assert result == 42
    assert round(time() - start_time, 1) == 0.6
    log('OK. delayed: %s', result)


@pytest.mark.asyncio
async def test_error(remote_mock, start_time):
    with pytest.raises(ZeroDivisionError):
        await (~ remote_mock.immediate_error())
    assert round(time() - start_time, 1) == 0.1
    log('OK. error: %s' % traceback.format_exc())


@pytest.mark.asyncio
async def test_delayed_error(remote_mock, start_time):
    with pytest.raises(ZeroDivisionError):
        await (~ remote_mock.delayed_error(0.5))
    assert round(time() - start_time, 1) == 0.6
    log('OK. error: %s' % traceback.format_exc())


@pytest.mark.asyncio
async def test_TimeoutError(remote_mock, start_time):
    with pytest.raises(TimeoutError):
        await remote_mock.delayed(2, '<X>').__send__(timeout=1)
    assert round(time() - start_time, 1) == 1
    log('OK. error: %s' % traceback.format_exc())


@pytest.mark.asyncio
async def test_HeartbeatError(pika_router, remote_mock, start_time):
    hbi = pika_router.heartbeat_interval
    asyncio.ensure_future(
        ~ pika_router.proxy(normalize(pika_router, 'remote:0')).sleep(hbi*5))
    await asyncio.sleep(hbi)
    with pytest.raises(router.HeartbeatError):
        await (~ remote_mock.immediate('<X>'))
    assert round(time() - start_time) == round(router.HEARTBEAT_ERROR)
    log('OK. error: %s' % traceback.format_exc())
    await pika_router.wait(normalize(pika_router, 'remote:0'), 2)


@pytest.mark.asyncio
async def test_multi(group_mock, start_time):
    result = await group_mock.delayed(0.5, 'x').__send__(multi=True)
    assert result == ['x', 'x']
    assert round(time() - start_time, 1) == 0.6
    log('OK. delayed multicast: %s' % result)


@pytest.mark.asyncio
async def test_broadcast(group_mock, remote_mock, start_time):
    remote_mock.some_property = 42
    result = await (~ group_mock.p_immediate(42))
    assert result == 'SUCCESS'
    assert round(time() - start_time) == 0
    log('OK. immediate broadcast: %s' % result)
    remote_mock.some_property = 0


@pytest.mark.asyncio
async def test_direct_NoResponse(remote_mock, start_time):
    with pytest.raises(router.NoResponse):
        await (~ remote_mock.p_immediate(42))
    assert round(time() - start_time, 1) <= 0.1
    log('OK. single NoResponse: %s' % traceback.format_exc())


@pytest.mark.asyncio
async def test_broad_NoResponse(group_mock, start_time):
    with pytest.raises(router.NoResponse):
        await (~ group_mock.p_immediate(42))
    assert round(time() - start_time, 1) == 0.1
    log('OK. multi NoResponse: %s' % traceback.format_exc())


@pytest.mark.asyncio
async def test_remote_equal(remote_mock):
    assertion = await (~(remote_mock.some_property == 0))
    assert assertion
    assertion = await (~(remote_mock.some_property == 1))
    assert not assertion


@pytest.mark.asyncio
async def test_property(remote_mock):
    result = await (~ remote_mock.some_property)
    assert result == 0

    await (~ remote_mock.set_some_property(1))
    result = await (~ remote_mock.some_property)
    assert result == 1

    remote_mock.some_property = 42
    await asyncio.sleep(0.1)
    result = await (~ remote_mock.some_property)
    assert result == 42

    remote_mock.other_property = await (~ remote_mock.some_property)
    await asyncio.sleep(0.1)
    result = await (~ remote_mock.other_property)
    assert result == 42

    result = await (~ (remote_mock.some_property + 1))
    assert result == 43
    remote_mock.some_property = 0


@pytest.mark.asyncio
async def test_nested(remote_mock, pika_router):
    result = await (~ remote_mock.immediate(remote_mock.some_property))
    assert result == 0

    remote_mock.some_property = 42
    await asyncio.sleep(0.1)
    result = await (~ remote_mock.immediate(remote_mock.some_property))
    assert result == 42

    result = await (~ remote_mock.immediate(
        pika_router.proxy(normalize(pika_router, 'remote:1')).mock.some_property))
    assert result == 0


@pytest.mark.asyncio
async def test_unpicklable(remote_mock):
    with pytest.raises(_pickle.PicklingError):
        await (~ remote_mock.unpicklable())


@pytest.mark.asyncio
async def test_add(remote_mock):
    remote_mock.some_property = 42
    await asyncio.sleep(0.1)
    result = await (~ remote_mock.immediate(remote_mock.some_property
                                           + remote_mock.some_property))
    assert result == 84
    assert (await (~ remote_mock.some_property)) == 42
    remote_mock.some_property = remote_mock.some_property + remote_mock.some_property
    await asyncio.sleep(0.1)
    assert (await (~ remote_mock.some_property)) == 84


@pytest.mark.asyncio
async def test_getitem(remote_mock):
    remote_mock.some_property = {'A': 1}
    assert (await (~ remote_mock.some_property['A'])) == 1


@pytest.mark.asyncio
async def test_setitem(remote_mock):
    remote_mock.some_property = {'A': 1}
    remote_mock.some_property['A'] = 2
    await asyncio.sleep(0.1)
    assert (await (~ remote_mock.some_property['A'])) == 2


@pytest.mark.asyncio
async def test_apply(remote_mock):
    remote_mock.some_property = ['foo', 'bar']
    assert (await (~ apply(tuple, remote_mock.some_property))) == ('foo', 'bar')
    await (~ apply(setattr, remote_mock, 'some_property', 42))
    assert (await (~ remote_mock.some_property)) == 42

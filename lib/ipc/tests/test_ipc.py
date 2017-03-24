import pickle
import logging
import traceback
from collections import namedtuple
from multiprocessing import Process
from time import time

import pytest
from twisted.internet.defer import inlineCallbacks, returnValue

from lib.ipc import get_router, TimeoutError
from lib.ipc import router
from lib.ipc.proxy import apply
from lib.ipc.gateways import basegw
from lib.ipc.route import normalize
from lib.ipc.util import async


from lib.ipc.gateways import pikagw
pikagw.MQ_HOST = 'localhost'

logging.getLogger().setLevel(logging.DEBUG)
router.HEARTBEAT_INTERVAL = 0.1
router.HEARTBEAT_ERROR = 0.6
basegw.GW_LOOP_INTERVAL = 0.01
log = logging.getLogger(__name__).info


if not hasattr(pytest, 'inlineCallbacks'):
    # hack to let child processes start without pytest-twisted plugin enabled.
    pytest.inlineCallbacks = inlineCallbacks


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

    @inlineCallbacks
    def delayed(self, timeout, value):
        yield async.sleep(timeout)
        returnValue(value)

    @inlineCallbacks
    def delayed_error(self, timeout):
        yield async.sleep(timeout)
        returnValue(1/0)

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


def init_router(component, pid=None, exch=None, local=False):
    r = get_router(component, pid=pid, exchange=exch)
    r.handlers.mock = CommandMock()
    r.start()
    if local:
        return r
    else:
        from twisted.internet import reactor
        @inlineCallbacks
        def sleep(t):
            r.stop()
            yield async.sleep(t)
            r.start()
        r.handlers.sleep = sleep
        r.handlers.reactor = reactor
        reactor.run()


@inlineCallbacks
def stop_remote(r, route):
    try:
        yield ~ r.proxy(route).reactor.stop()
    except router.HeartbeatError:
        pass


@pytest.yield_fixture(scope='module')
def pika_router():
    Process(target=init_router, args=('remote', 0, 'PIKA_IPC_TEST')).start()
    Process(target=init_router, args=('remote', 1, 'PIKA_IPC_TEST')).start()
    r = init_router('local', exch='PIKA_IPC_TEST', local=True)
    pytest.blockon(async.wait_true(lambda: r.ready))
    remote0 = normalize(r, 'remote:0')
    remote1 = normalize(r, 'remote:1')
    pytest.blockon(r.wait(remote0, 100))
    pytest.blockon(r.wait(remote1, 100))
    log('proxy repr: %s', r.proxy(remote0).mock)
    yield r
    pytest.blockon(stop_remote(r, remote0))
    pytest.blockon(stop_remote(r, remote1))
    r.stop()


@pytest.fixture
def remote_mock(pika_router):
    return pika_router.proxy(normalize(pika_router, 'remote:0')).mock


@pytest.fixture
def group_mock(pika_router):
    return pika_router.proxy(normalize(pika_router, 'remote'), targets=2).mock


@pytest.fixture
def start_time():
    return time()


@pytest.inlineCallbacks
def test_immediate(remote_mock, start_time):
    result = yield ~ remote_mock.immediate('<X>')
    assert round(time() - start_time, 1) == 0
    log('OK. immediate: %s', result)


@pytest.inlineCallbacks
def test_delayed(remote_mock, start_time):
    result = yield ~ remote_mock.delayed(0.5, 42)
    assert result == 42
    assert round(time() - start_time, 1) == 0.5
    log('OK. delayed: %s', result)


@pytest.inlineCallbacks
def test_error(remote_mock, start_time):
    with pytest.raises(ZeroDivisionError):
        yield ~ remote_mock.immediate_error()
    assert round(time() - start_time, 1) == 0
    log('OK. error: %s' % traceback.format_exc())


@pytest.inlineCallbacks
def test_delayed_error(remote_mock, start_time):
    with pytest.raises(ZeroDivisionError):
        yield ~ remote_mock.delayed_error(0.5)
    assert round(time() - start_time, 1) == 0.5
    log('OK. error: %s' % traceback.format_exc())


@pytest.inlineCallbacks
def test_TimeoutError(remote_mock, start_time):
    with pytest.raises(TimeoutError):
        yield remote_mock.delayed(2, '<X>').__send__(timeout=1)
    assert round(time() - start_time, 1) == 1
    log('OK. error: %s' % traceback.format_exc())
    # yield async.sleep(1)


@pytest.inlineCallbacks
def test_HeartbeatError(pika_router, remote_mock, start_time):
    hbi = pika_router.heartbeat_interval
    ~ pika_router.proxy(normalize(pika_router, 'remote:0')).sleep(hbi*5)
    with pytest.raises(router.HeartbeatError):
        yield ~ remote_mock.immediate('<X>')
    assert round(time() - start_time) == round(router.HEARTBEAT_ERROR)
    log('OK. error: %s' % traceback.format_exc())
    yield pika_router.wait(normalize(pika_router, 'remote:0'), 2)


@pytest.inlineCallbacks
def test_multi(group_mock, start_time):
    result = yield group_mock.delayed(0.5, 'x').__send__(multi=True)
    assert result == ['x', 'x']
    assert round(time() - start_time, 1) == 0.5
    log('OK. delayed multicast: %s' % result)


@pytest.inlineCallbacks
def test_broadcast(group_mock, remote_mock, start_time):
    remote_mock.some_property = 42
    result = yield ~ group_mock.p_immediate(42)
    assert result == 'SUCCESS'
    assert round(time() - start_time) == 0
    log('OK. immediate broadcast: %s' % result)
    remote_mock.some_property = 0


@pytest.inlineCallbacks
def test_direct_NoResponse(remote_mock, start_time):
    with pytest.raises(router.NoResponse):
        yield ~ remote_mock.p_immediate(42)
    assert round(time() - start_time, 1) <= 0.1
    log('OK. single NoResponse: %s' % traceback.format_exc())


@pytest.inlineCallbacks
def test_broad_NoResponse(group_mock, start_time):
    with pytest.raises(router.NoResponse):
        yield ~ group_mock.p_immediate(42)
    assert round(time() - start_time, 1) == 0
    log('OK. multi NoResponse: %s' % traceback.format_exc())


@pytest.inlineCallbacks
def test_remote_equal(remote_mock):
    assertion = yield ~(remote_mock.some_property == 0)
    assert assertion
    assertion = yield ~(remote_mock.some_property == 1)
    assert not assertion


@pytest.inlineCallbacks
def test_property(remote_mock):
    result = yield ~ remote_mock.some_property
    assert result == 0

    yield ~ remote_mock.set_some_property(1)
    result = yield ~ remote_mock.some_property
    assert result == 1

    remote_mock.some_property = 42
    result = yield ~ remote_mock.some_property
    assert result == 42

    remote_mock.other_property = yield ~ remote_mock.some_property
    result = yield ~ remote_mock.other_property
    assert result == 42

    result = yield ~ (remote_mock.some_property + 1)
    assert result == 43
    remote_mock.some_property = 0


@pytest.inlineCallbacks
def test_nested(remote_mock, pika_router):
    result = yield ~ remote_mock.immediate(remote_mock.some_property)
    assert result == 0

    remote_mock.some_property = 42
    result = yield ~ remote_mock.immediate(remote_mock.some_property)
    assert result == 42

    result = yield ~ remote_mock.immediate(
        pika_router.proxy(normalize(pika_router, 'remote:1')).mock.some_property)
    assert result == 0


@pytest.inlineCallbacks
def test_unpicklable(remote_mock):
    with pytest.raises(pickle.PickleError):
        yield ~ remote_mock.unpicklable()


@pytest.inlineCallbacks
def test_add(remote_mock):
    remote_mock.some_property = 42
    result = yield ~ remote_mock.immediate(remote_mock.some_property
                                           + remote_mock.some_property)
    assert result == 84
    assert (yield ~ remote_mock.some_property) == 42
    remote_mock.some_property = remote_mock.some_property + remote_mock.some_property
    assert (yield ~ remote_mock.some_property) == 84


@pytest.inlineCallbacks
def test_getitem(remote_mock):
    remote_mock.some_property = {'A': 1}
    assert (yield ~ remote_mock.some_property['A']) == 1


@pytest.inlineCallbacks
def test_setitem(remote_mock):
    remote_mock.some_property = {'A': 1}
    remote_mock.some_property['A'] = 2
    assert (yield ~ remote_mock.some_property['A']) == 2


@pytest.inlineCallbacks
def test_apply(remote_mock):
    remote_mock.some_property = ['foo', 'bar']
    assert (yield ~ apply(tuple, remote_mock.some_property)) == ('foo', 'bar')
    yield ~ apply(setattr, remote_mock, 'some_property', 42)
    assert (yield ~ remote_mock.some_property) == 42

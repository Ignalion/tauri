import atexit
import pickle
import logging
import traceback
import uuid
from copy import deepcopy
from functools import partial
from threading import Thread
from time import time

from twisted.internet import reactor
from twisted.internet.defer import Deferred, maybeDeferred, inlineCallbacks
from twisted.internet.task import LoopingCall
from twisted.internet.threads import deferToThread

from .proxy import Proxy, Operation, BlockingProxy
from .util import async
from .util.colls import flatten
from .util.types import Namespace, ReprHook

HEARTBEAT_INTERVAL = 1
HEARTBEAT_WARNING = 1
HEARTBEAT_ERROR = 5
HB_KWARGS = {'oneshot': True, 'metadata': {'quiet': True}}


class RESPONSE:
    OK = 0
    ERROR = 1


class MTYPE:
    REQUEST = 0
    ONESHOT = 1


class HeartbeatError(Exception):
    """Request is not being processed by any router"""


class RemoteException(Exception):
    """There is an exception upon remote request processing"""


class NoResponse(Exception):
    """Particular router can not process this request"""


class MultipleResponse(Exception):
    """Unexpected additional responses in single mode"""


class TimeoutError(Exception):
    pass


class Router(object):

    def __init__(self, gateway, master=None):
        logging.debug('Initializing Router with <%s>' % gateway)

        self.sent = {}
        self.processing = {}
        self.processing_metadata = {}
        self.gw = gateway
        self.gw.bind(self.receive)
        self.master = master
        self.running = False
        self.heartbeat_interval = HEARTBEAT_INTERVAL
        self._heartbeat_loop = LoopingCall(self._heartbeat)

        self.handlers = Namespace()
        self.handlers.router = self

    # process interface:

    @inlineCallbacks
    def start(self):
        """Start processing incoming messages."""
        if not self.running:
            logging.debug('Starting %s' % self.__class__.__name__)
            self.running = True
            yield async.wait_true(lambda: self.ready)
            self._heartbeat_loop.start(self.heartbeat_interval)

    def stop(self):
        """Stop processing incoming messages."""
        if self.running:
            self.running = False
            self._heartbeat_loop.stop()

    @property
    def ready(self):
        return self.running and getattr(self.gw, 'connected', True)

    # user interface:

    def proxy(self, route, **kwargs):
        """Receive proxy for the remote router (or group).

        Arguments will be passed to all requests through this proxy.
        """
        return Proxy(partial(self.request, route, **kwargs))

    def request(self, route, op, targets=1,
                multi=False, oneshot=False, timeout=None, metadata=None):
        """Send remote request.

        In most cases you won't need to call this method directly
        (and construct operation chain for it), consider using proxy instead.

        Args:
            route (str): route to the remote router (or group)
            op (Operation): operation chain to be applied.
            targets (int): amount of the expected remote destinations.
                0 - if unknown (multi-response mode is not available).
            multi (bool): expect response from each target (default: only one)
            oneshot (bool): do not expect answer.
            timeout (int): timeout in seconds to rise TimeoutError
                if request is still in progress, no timeout if None
            metadata: request metadata

        Returns:
            Deferred or None (for oneshot=True)
        """
        assert isinstance(op, Operation), 'op should be an Operation instance'
        if not targets and not timeout:
            assert not multi, 'multi-mode can only be used ' \
                              'with known amount of targets or with timeout'

        metadata = metadata or {}
        old = metadata.get('old_protocol')

        if oneshot:
            body = (None, op, metadata) if not old else op
            self.gw.send(route, MTYPE.ONESHOT, body)
        else:
            uid = str(uuid.uuid4())
            self.sent[uid] = {
                'deferred': Deferred(),
                'heartbeat': time(),
                'targets': targets,
                'multi': multi,
                'responses': [],
                'on_timeout': timeout and
                reactor.callLater(timeout, self._on_timeout, uid)
            }
            body = (uid, op, metadata) if not old else (uid, op)
            self.gw.send(route, MTYPE.REQUEST, body)
            return self.sent[uid]['deferred']

    def wait(self, route, timeout):
        """Wait for the route to appear.

        Args:
            route (str): route to the remote router (or group).
            timeout: seconds to wait, raises TimeoutError afterwards.

        Returns:
            Deferred.
        """
        return async.wait_true(self.proxy(route).router.ready.__send__,
                               timeout=timeout, ignore_exc=HeartbeatError)

    # chanel interface:

    def receive(self, requester, message_type, body):
        """Receive remote message and handle it's command if possible

        This method should be called by the gateway as a callback for incoming
        message.
        """
        if self.running:

            if isinstance(body, tuple) and len(body) == 3:
                uid, op, metadata = body
            else: # old protocol backward compatibility, fixme: remove
                metadata = {'old_protocol': True}
                uid, op = body if message_type == MTYPE.REQUEST else (None, body)

            if message_type == MTYPE.REQUEST:
                self.processing.setdefault(requester, set()).add(uid)
                self.processing_metadata[uid] = metadata
                d = maybeDeferred(self._eval_operation, op)
                d.addCallbacks(_recv_callback, _recv_errback)
                d.addCallback(self._send_back, requester, uid, metadata)
            elif message_type == MTYPE.ONESHOT:
                try:
                    self._eval_operation(op)
                except Exception:
                    logging.error('oneshot request processing error:\n%s',
                                  traceback.format_exc())

    # callbacks for receiver:

    def on_response(self, uid, result):
        if uid in self.sent:
            req = self.sent[uid]
            multi = req['multi']
            targets = req['targets']
            responses = req['responses']
            responses.append(_extract_result(result))
            any_responses = any(map(_is_response, responses))
            done = targets and len(responses) == targets
            if (not targets and not multi and any_responses) or done:
                _return(req)
                del self.sent[uid]
        else:
            logging.warning('Response for missed request: %s', uid)

    def on_heartbeat(self, uids):
        for uid in uids:
            if uid in self.sent:
                self.sent[uid]['heartbeat'] = time()
            else:
                logging.warning('Heartbeat for missed request: %s', uid)

    # request-processing internals:

    def _eval_operation(self, op):
        result = op.__eval__(self.handlers)
        if isinstance(result, Proxy):
            return ~ result
        return result

    def _send_back(self, result, requester, uid, metadata, as_errback=False):
        del self.processing_metadata[uid]
        back_proxy = self.proxy(requester, oneshot=True, metadata=metadata)
        d = ~ back_proxy.router.on_response(uid, result)
        d.addCallback(lambda _: self.processing[requester].remove(uid))
        if not as_errback:
            d.addErrback(
                lambda f: self._send_back(_recv_errback(f), requester,
                                          uid, metadata, True))

    def _on_timeout(self, uid):
        if uid in self.sent:
            req = self.sent.pop(uid)
            req['deferred'].errback(TimeoutError('Request timed out',
                                                 *req['responses']))

    def _heartbeat(self):
        # notify master of our presence (if any)
        if self.master:
            ~ self.proxy(self.master, **HB_KWARGS).master.notify(self.gw.route)

        # send heartbeats for requests being processed by us
        for requester, uids in self.processing.items():
            if uids:
                kwargs = deepcopy(HB_KWARGS)
                if any(m.get('old_protocol')
                       for m in [self.processing_metadata[uid] for uid in uids]):
                    kwargs['metadata']['old_protocol'] = True
                ~ self.proxy(requester, **kwargs).router.on_heartbeat(uids)

        # check our own requests for some missed heartbeats
        now = time()
        for uid, request in list(self.sent.items()):
            if now > request['heartbeat'] + HEARTBEAT_WARNING:
                logging.warning('Response is overdue %s seconds',
                                (now - request['heartbeat']))
            if now > request['heartbeat'] + HEARTBEAT_ERROR:
                request['deferred'].errback(HeartbeatError(uid))
                del self.sent[uid]


def _is_response(x):
    return not isinstance(x, NoResponse)


def _extract_result(result):
    code, value = result
    if code == RESPONSE.OK:
        return value
    elif code == RESPONSE.ERROR:
        err, tb = value
        try:
            err_class = pickle.loads(err).__class__
        except:
            err_class = RemoteException
        if issubclass(err_class, KeyError):
            tb = ReprHook(tb)
        return err_class(tb)
    else:
        raise ValueError('Unknown response code')


def _recv_callback(r):
    return RESPONSE.OK, r


def _recv_errback(f):
    if not isinstance(f.value, NoResponse):
        logging.critical(f)
    return RESPONSE.ERROR, (pickle.dumps(f.value), f.getTraceback())


def _return(request):
    d = request['deferred']
    responses = request['responses']
    if not request['multi']:
        results = list(filter(_is_response, responses))
        if len(results) == 0:
            d.errback(NoResponse())
        elif len(results) > 1 and any(r != results[0] for r in results[1:]):
            d.errback(MultipleResponse(*results))
        else:
            result = results[0]
            if isinstance(result, Exception):
                d.errback(result)
            else:
                d.callback(result)
    else:
        if any(isinstance(r, Exception) for r in responses):
            d.errback(RemoteException(responses))
        else:
            iterize = lambda x: x if isinstance(x, list) else [x]
            d.callback(flatten(list(map(iterize, responses))))


class BlockingRouter(Router):

    @async.blockingDeferredCall
    def start(self, *args, **kwargs):
        if not reactor.running:
            thread = Thread(target=reactor.run, args=(False,))
            thread.daemon = True
            thread.start()
            atexit.register(self.stop)
        return super(BlockingRouter, self).start()

    def stop(self):
        super(BlockingRouter, self).stop()
        if reactor.running:
            reactor.callFromThread(reactor.stop)

    def proxy(self, route, **kwargs):
        return BlockingProxy(partial(self.request, route, **kwargs))

    @async.blockingDeferredCall
    def request(self, *args, **kw):
        return maybeDeferred(super(BlockingRouter, self).request, *args, **kw)


class ThreadingRouter(Router):

    def _eval_in_thread(self, op):
        x = op.__eval__(self.handlers)
        if isinstance(x, Deferred):
            x = async.blockOnDeferred(x)
        return x

    def _eval_operation(self, op):
        return deferToThread(self._eval_in_thread, op)

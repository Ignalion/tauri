import pickle
import logging
import traceback

import pika
from twisted.internet.defer import inlineCallbacks
from ..router import LoopingCall  # FIXME
from pika.adapters.twisted_connection import TwistedConnection

from .basegw import BaseGateway
from ..route import subroutes
from ..router import MTYPE
from ..util.types import enum2dict
from ..util.colls import swap

try:
    import BigWorld
except ImportError:
    BigWorld = None


logging.getLogger('pika').setLevel(logging.INFO)
log = logging.getLogger(__name__)

TYPE_NAMES = swap(enum2dict(MTYPE))
MAX_LOG_BODY_SIZE = 1024


def _get_metadata(body, key, default=None):
    if isinstance(body, tuple) and len(body) == 3:
        return body[2].get(key, default)
    else:
        return default


class BasePikaGateway(BaseGateway):

    def __init__(self, route, mq_host, exchange):
        super(BasePikaGateway, self).__init__()
        self.route = route
        self.exchange = exchange
        self.params = pika.ConnectionParameters(host=mq_host)
        self.connected = False
        self.connection = None
        self.channel = None
        self.queue = None
        logging.debug('Initializing %s with route <%s> on exchange %s at host %s' %
                      (self.__class__.__name__, route, exchange, mq_host))
        self._connect()

    def _connect(self):
        raise NotImplementedError

    def send(self, to, mtype, body):
        from_ = self.route
        if not _get_metadata(body, 'quiet'):
            logging.debug('Sending %s -> %s : [%s] %s',
                          from_, to, TYPE_NAMES[mtype],
                          str(body)[:MAX_LOG_BODY_SIZE])
        message_body = pickle.dumps((from_, mtype, body),
                                     protocol=-1)
        assert self.channel, 'Not connected'
        self.channel.basic_publish(self.exchange, to, message_body)

    def _on_message(self, ch, method, props, body):
        try:
            from_, mtype, body = pickle.loads(body)
        except Exception as e:
            log.error('Could not load message body: %s\n%s',
                      e.message, traceback.format_exc())
            return
        if not _get_metadata(body, 'quiet'):
            logging.debug('Received %s -> %s : [%s] %s',
                          from_, self.route, TYPE_NAMES[mtype],
                          str(body)[:MAX_LOG_BODY_SIZE])
        self.receive(from_, mtype, body)


class TwistedPikaGateway(BasePikaGateway):

    def _connect(self):
        self.connection = TwistedConnection(self.params, self._on_connected)

    @inlineCallbacks
    def _on_connected(self, connection):
        log.debug('connected: %s', connection)
        self.channel = yield connection.channel()
        yield self.channel.exchange_declare(exchange=self.exchange,
                                            auto_delete=True)
        log.debug('channel opened: %s', self.channel)
        self.channel.basic_qos(prefetch_count=1)
        self.queue = (
            yield self.channel.queue_declare(exclusive=True)).method.queue
        log.debug('queue declared: %s', self.queue)
        for subroute in subroutes(self.route):
            yield self.channel.queue_bind(queue=self.queue,
                                          exchange=self.exchange,
                                          routing_key=subroute)
        yield self.channel.basic_qos(prefetch_count=1)
        dqueue, _ = yield self.channel.basic_consume(queue=self.queue,
                                                     no_ack=True)
        self.connected = True
        self._process(dqueue)

    @inlineCallbacks
    def _process(self, dqueue):
        while True:
            self._on_message(*(yield dqueue.get()))


class BlockingPikaGateway(BasePikaGateway):
    def _connect(self):
        self.connection = pika.BlockingConnection(self.params)
        self.channel = channel = self.connection.channel()
        channel.exchange_declare(exchange=self.exchange, auto_delete=True)
        self.queue = channel.queue_declare(exclusive=True).method.queue
        for subroute in subroutes(self.route):
            channel.queue_bind(self.queue, self.exchange, subroute)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(self._on_message, no_ack=True, queue=self.queue)
        self.connected = True

    def _process(self):
        self.connection.process_data_events()


class LoopingPikaGateway(BlockingPikaGateway):

    def _connect(self):
        super()._connect()
        self.loop = LoopingCall(self._process)
        self.loop.start(0.1)


if BigWorld:
    PikaGateway = LoopingPikaGateway
else:
    PikaGateway = LoopingPikaGateway

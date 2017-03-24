import logging
import time

from twisted.internet.task import LoopingCall

from . import get_router, route
from .util.async import wait_true
from .util.colls import get_path, del_path
from .util.funcs import log_exc

log = logging.getLogger(__name__)


class Master(object):

    def __init__(self):
        self.network = {}
        self.active_routes = {}
        self.route_expire_time = 5
        self.loop = LoopingCall(self._loop)
        self.loop.start(1)

    def notify(self, r):
        route_parts = route.split(r)
        assert len(route_parts) > 1, 'Route must have at least 2 parts'
        assert route_parts[-1].isdigit(), 'Last route part must be an integer'
        if r not in self.active_routes:
            log.info('new route registered: %s', r)
        self.active_routes[r] = time.time()
        node = self.network
        for rpart in route_parts[:-2]:
            node = node.setdefault(rpart, {})
        ids = node.setdefault(route_parts[-2], set())
        ids.add(int(route_parts[-1]))

    def wait(self, r, interval=0.1, timeout=30):
        return wait_true(lambda: r in self.active_routes, interval, timeout,
                         msg='route wait timeout expired for: %s' % r)

    def unregister(self, r):
        if r in self.active_routes:
            del self.active_routes[r]
            route_parts = route.split(r)
            ids = get_path(self.network, route_parts[:-1])
            ids.remove(int(route_parts.pop()))
            while route_parts and not get_path(self.network, route_parts):
                del_path(self.network, route_parts)
                route_parts.pop()
        else:
            log.warning('attempt to un-register non-existent route: %s', r)

    @log_exc(log)
    def _loop(self):
        for r, last_time in list(self.active_routes.items()):
            if time.time() - last_time > self.route_expire_time:
                log.info('route has gone: %s', r)
                self.unregister(r)


if __name__ == '__main__':
    import sys
    from twisted.internet import reactor
    from twisted.internet.defer import inlineCallbacks

    logging.basicConfig(level=logging.DEBUG)

    @inlineCallbacks
    def run():
        if len(sys.argv) > 1:
            token = sys.argv[1]
        else:
            token = ''
        router = get_router('master', token=token)
        yield router.start()
        node = Master()
        router.handlers.notify = node.notify

    reactor.callLater(0, run)
    reactor.run()

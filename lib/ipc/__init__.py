import logging

from . import route
from .router import Router, BlockingRouter
from .router import HeartbeatError, RemoteException,\
    NoResponse, MultipleResponse, TimeoutError
from .gateways.pikagw import BasePikaGateway, PikaGateway
from .util.funcs import repr_args

MQ_EXCHANGE = 'ROOT'  # FIXME Remove this from here


router_cache = {}


def get_router(component, pid=None, token=None, master=None, router_cls=Router,
               gw_cls=PikaGateway, **gw_kw):

    own_route = route.get_route(component, pid, token)

    if own_route not in router_cache:
        master_route = (route.join(route.split(own_route)[0], master)
                        if master else None)

        if issubclass(gw_cls, BasePikaGateway):
            if not gw_kw.get('exchange'):
                gw_kw['exchange'] = MQ_EXCHANGE

        logging.info('starting IPC router at [%s] <-> %s(%s)',
                     own_route, gw_cls.__name__, repr_args(**gw_kw))

        router_cache[own_route] = router_cls(gw_cls(own_route, **gw_kw),
                                             master=master_route)

    router = router_cache[own_route]
    assert router.__class__ is router_cls
    assert router.gw.__class__ is gw_cls
    return router


def get_blocking_router(*args, **kwargs):
    router = get_router(*args, router_cls=BlockingRouter, **kwargs)
    router.start()
    return router

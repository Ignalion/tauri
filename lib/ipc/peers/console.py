import logging

from testcore.ipc import get_router
from testcore.ipc.router import BlockingRouter

logging.basicConfig(level=logging.INFO)

router = None


def init():
    global router
    router = get_router('console', router_cls=BlockingRouter)
    router.start()
    return router


if __name__ == '__main__':
    init()
    import IPython
    IPython.embed()

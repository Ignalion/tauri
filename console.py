import asyncio

from utils.log import init_logging
from common.consts import MQ_URL, EXCHANGE
from lib.ipc import get_router

init_logging()
router = None


async def init():
    global router
    router = get_router('console', exchange=EXCHANGE, mq_host=MQ_URL)
    await router.start()
    return router


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init())
    import IPython
    IPython.embed(loop_runner='asyncio')

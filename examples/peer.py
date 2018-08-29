import asyncio
import logging

from lib.ipc import get_router
from lib.ipc.gateways import pikagw
pikagw.MQ_HOST = '127.0.0.1'

logging.basicConfig(level=logging.DEBUG)


async def delayed(timeout, value):
    await asyncio.sleep(timeout)
    return value


async def main():
    r = get_router('asyncio', token='ipc_test')
    r.handlers.get_42 = lambda: 42
    r.handlers.get_delayed = delayed
    await r.start()

loop = asyncio.get_event_loop()
asyncio.ensure_future(main())
loop.run_forever()

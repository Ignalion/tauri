import asyncio
import logging

from lib.ipc import get_router
from lib.ipc.gateways import pikagw
pikagw.MQ_HOST = '127.0.0.1'

PEER_ROUTE = 'ipc_test:asyncio'


logging.basicConfig(level=logging.DEBUG)

async def main():
    r = get_router('client')
    await r.start()
    result = await (~r.proxy(PEER_ROUTE).get_42())
    print('GOT RESULT: %s' % result)
    result = await (~r.proxy(PEER_ROUTE).get_delayed(1, 45))
    print('GOT RESULT: %s' % result)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

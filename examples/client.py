import asyncio
import logging

from lib.ipc import get_router
from lib.ipc.proxy import apply

MQ_HOST = '127.0.0.1'

PEER_ROUTE = 'game:main'


logging.basicConfig(level=logging.DEBUG)


async def main():
    r = get_router('client', exchange='ROOT', mq_host=MQ_HOST)
    await r.start()
    result = await (~r.proxy(PEER_ROUTE).add_point())
    print('GOT RESULT: %s' % result)
    result = await (~apply(list, r.proxy(PEER_ROUTE).entities))
    print('GOT RESULT: %s' % result)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

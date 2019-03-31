import asyncio
import logging
import time

from common.consts import TICK_INTERVAL

log = logging.getLogger(__name__)

ENTITIES = {}


async def tick():
    for entity in ENTITIES.values():
        await asyncio.coroutine(entity.tick)()


def run():
    loop = asyncio.get_event_loop()

    # FIXME I think we need a function for it
    log.info('Creating api')
    from entities.api import API
    from common.consts import MQ_URL, EXCHANGE
    loop.run_until_complete(create_entity(API, MQ_URL, EXCHANGE))

    log.info('Starting main loop')
    while True:
        before_tick = time.time()
        loop.run_until_complete(tick())
        after_tick = time.time()

        tick_time = after_tick - before_tick
        tick_remaining = TICK_INTERVAL - tick_time
        if tick_remaining < 0:
            log.warning('Tick took %s', tick_time)

        loop.run_until_complete(asyncio.sleep(tick_remaining))


async def create_entity(cls, *args, **kwargs):
    entity = cls()

    global ENTITIES
    ENTITIES[entity.id] = entity

    await asyncio.coroutine(entity._init)(*args, **kwargs)
    return entity

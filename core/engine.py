import asyncio
import logging
import time

from common.consts import TICK_INTERVAL

log = logging.getLogger(__name__)

ENTITIES = {}
ENTITY_TYPES = {}


async def tick():
    for entity in ENTITIES.values():
        await asyncio.coroutine(entity.tick)()


def get_singleton(name):
    singleton_ids = ENTITY_TYPES[name]
    assert len(singleton_ids) == 1, 'Singleton should be only one'
    return ENTITIES[singleton_ids[0]]


def run():
    loop = asyncio.get_event_loop()

    # FIXME I think we need a function for it
    log.info('Creating api')
    from entities.api import API
    from entities.environment import Environment
    from common.consts import MQ_URL, EXCHANGE
    loop.run_until_complete(create_entity(API, MQ_URL, EXCHANGE))
    log.info('Creating environment')
    loop.run_until_complete(create_entity(Environment))

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
    global ENTITY_TYPES
    ENTITIES[entity.id] = entity
    ENTITY_TYPES.setdefault(entity.name, []).append(entity.id)

    await asyncio.coroutine(entity._init)(*args, **kwargs)
    return entity

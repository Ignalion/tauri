from itertools import count
import logging

from common.consts import START_ID

log = logging.getLogger(__name__)

ENTITY_COUNTER = count(START_ID)


class Entity:
    def __init__(self):
        self.id = next(ENTITY_COUNTER)
        name = self.__class__.__name__
        self.name = name
        log.info('Entity %s:%s created', self.id, name)

    def _init(self, *args, **kwargs):
        log.info('Init of entity %s', self.id)

    def tick(self):
        pass

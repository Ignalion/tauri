from core import engine
from core.entity import Entity
from entities.point import Point
from lib.ipc import get_router


class API(Entity):
    async def _init(self, url, exchange):
        super()._init()
        self.router = get_router('main', token='game', exchange=exchange,
                                 mq_host=url)
        self.router.handlers.add_point = self.add_point
        self.router.handlers.entities = engine.ENTITIES
        await self.router.start()

    async def add_point(self, x=0, y=0):
        await engine.create_entity(Point, x=x, y=y)

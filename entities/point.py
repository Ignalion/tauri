from core.entity import Entity


class Point(Entity):
    def _init(self, x=0, y=0):
        super()._init()
        self.x = x
        self.y = y

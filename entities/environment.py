import logging
import random

from core.entity import Entity
from common.consts import MAX_X, MAX_Y, GRASS_PRC, GRASS_GROW

logger = logging.getLogger(__name__)


def generate_grass():
    # Creating random Dirt and Rock if randomize is set
    grass = {}
    for i in range(MAX_X):
        for j in range(MAX_Y):
            r = random.random()
            if r < GRASS_PRC:
                grass[(i, j)] = 1
    return grass


class Environment(Entity):

    def _init(self):
        self.grass_map = generate_grass()
        self.counter = 0

    def _grass_grow(self):
        if self.counter == GRASS_GROW:
            self.counter = 0
            for cell in self.grass_map:
                if not self.grass_map[cell]:
                    self.grass_map[cell] = 1

    def get_aoi(self, entity):
        grass = []
        for x in range(entity.x - entity.aoi_range, entity.x + entity.aoi_range):
            for y in range(entity.y - entity.aoi_range, entity.y + entity.aoi_range):
                if self.grass_map.get((x, y)):
                    grass.append((x, y))
        return grass

    def tick(self):
        self._grass_grow()


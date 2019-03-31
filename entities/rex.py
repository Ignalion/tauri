import logging
import random

from core.engine import get_singleton
from core.entity import Entity
from common.consts import SEX, MAX_X, MAX_Y, REX_AOI, GRASS_NUTRITION
from utils.math import sign
from utils.colls import attrs

logger = logging.getLogger(__name__)


class Rex(Entity):
    def _init(self, sex=None, pos=None):
        super()._init()
        self.sex = sex if sex else getattr(SEX, random.choice(attrs(SEX)))
        self.x, self.y = pos if pos else (random.randint(1, MAX_X),\
                                          random.randint(1, MAX_Y))
        self.aoi_range = REX_AOI
        self.env = get_singleton('Environment')
        self.hunger = 0
        self.alive = True

    def make_random_move(self):
        self.x += random.randint(-1, 1)
        self.y += random.randint(-1, 1)

    def move(self):
        logger.trace('Calculating aoi')
        grass_list = self.env.get_aoi(self)
        logger.trace('AOI calculated')
        if grass_list:
            target = random.choice(grass_list)
            logger.debug('%d Moving to target %s', self.id, target)
            self.x += sign(target[0] - self.x)
            self.y += sign(target[1] - self.y)
        else:
            self.make_random_move()

    def try_to_eat(self):
        food = self.env.grass_map.get((self.x, self.y))
        if food:
            self.env.grass_map[(self.x, self.y)] = 0
            self.hunger = 0 if self.hunger < GRASS_NUTRITION else \
                self.hunger - GRASS_NUTRITION
            logger.debug('Ate a plant')

    def tick(self):
        # if not self.alive:
        #     return
        if self.hunger == 100:
            self.alive = False
        self.move()
        self.try_to_eat()
        self.hunger += 1
        logger.debug('Hunger level: %s', self.hunger)
        logger.debug('%d Moved to %s', self.id, (self.x, self.y))


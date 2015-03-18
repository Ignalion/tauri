#!/usr/bin/env python3

import random

MAXX = 5
MAXY = 10

DIRT = 0.7

MAP = {
    'ROCK': 0,
    'DIRT': 1,
    'GRASS': 2,
}


class World:
    """
    """

    def __init__(self, randomize=True):
        self.objects = []
        self.map = [[0 for i in range(MAXY)] for j in range(MAXX)]
        self._counter = 0

        # Creating random Dirt and Rock if randomize is set
        if randomize:
            for i in range(MAXX):
                for j in range(MAXY):
                    r = random.random()
                    self.map[i][j] = MAP['DIRT'] if r < DIRT else MAP['ROCK']

    def run(self):
        # Main loop
        while True:
            # Iterating over all objects pool
            for obj in self.objects:
                obj.step()

        # Make grass grow
        if self._counter == 100:
            self._counter = 0
            for i, line in enumerate(self.map):
                for j, cell in enumerate(line):
                    if cell == 1:
                        self.map[i][j] = 2
        else:
            self._counter += 1

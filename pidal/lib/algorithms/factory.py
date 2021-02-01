import random
from typing import Callable


class Factory(object):

    algorithms = {
            "mod": lambda v1, v2: int(v2) % int(v1),
            "random": lambda: random.randint(0, 100),
            }

    @classmethod
    def new(cls, algorithm: str) -> Callable:
        a = cls.algorithms.get(algorithm, None)
        if not a:
            raise Exception("unkonwn algorithm [{}].".format(algorithm))
        return a

import abc
from typing import Awaitable


class Stream(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def read_bytes(self, num_bytes: int) -> bytes:
        pass

    @abc.abstractmethod
    async def write(self, data: bytes):
        pass

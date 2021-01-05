import abc

import tornado.iostream


class Stream(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def read_bytes(self, num_bytes: int) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    async def write(self, data: bytes):
        raise NotImplementedError()


class IOStream(Stream):

    def __init__(self, stream: tornado.iostream.IOStream):
        self._stream = stream

    async def read_bytes(self, num_bytes: int) -> bytes:
        return await self._stream.read_bytes(num_bytes)

    async def write(self, data: bytes):
        return await self._stream.write(data)

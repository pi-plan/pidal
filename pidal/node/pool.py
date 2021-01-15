import asyncio
import collections

from typing import Set

import async_timeout

from pidal.node.connection import Connection


class Pool(asyncio.AbstractServer):

    def __init__(self,
                 minsize: int,
                 maxsize: int,
                 dsn: str,
                 timeout: int = 10,
                 recycle: int = 0):
        self.minsize: int = minsize
        self.maxsize: int = maxsize
        self.timeout: int = timeout
        self.recycle: int = recycle
        self.dsn: str = dsn

        self.loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

        self._acquiring: int = 0
        self._free = collections.deque(maxlen=maxsize or None)
        self._cond = asyncio.Condition()
        self._used: Set[Connection] = set()
        self._closing: bool = False
        self._closed: bool = False

    @classmethod
    async def new(cls, minsize: int, maxsize: int, timeout=10, **kwargs):
        p = cls(minsize, maxsize, timeout, **kwargs)
        if p.minsize > 0:
            await p.fill_free()
        return p

    @property
    def size(self):
        return len(self._free) + len(self._used) + self._acquiring

    async def fill_free(self, override_min: bool = False):
        n, free = 0, len(self._free)
        while n < free:
            conn = self._free[-1]
            if conn.closed:
                self._free.pop()
            elif 0 < self.recycle < self.loop.time() - conn.last_usage:
                conn.close()
                self._free.pop()
            else:
                self._free.rotate()
            n += 1

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await Connection.new(self.dsn)
                self._free.append(conn)
            finally:
                self._acquiring -= 1
        if self._free:
            return

        if override_min and self.size < self.maxsize:
            self._acquiring += 1
            try:
                conn = await Connection.new(self.dsn)
                self._free.append(conn)
            finally:
                self._acquiring -= 1

    def close(self):
        if self._closed:
            return
        self._closing = True

    async def wait_closed(self):
        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called "
                               "after .close()")

        while self._free:
            conn = self._free.popleft()
            conn.close()

        async with self._cond:
            # 确保所有的都关闭
            while self.size > len(self._free):
                await self._cond.wait()

        self._closed = True

    async def _wakeup(self):
        async with self._cond:
            self._cond.notify()

    async def acquire(self) -> Connection:
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        async with async_timeout.timeout(self.timeout), self._cond:
            while True:
                await self.fill_free(True)
                if self._free:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    await self._cond.wait()

    def release(self, conn: Connection):
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.is_closed():
            if self._closing:
                conn.close()
            else:
                conn.clean()
                self._free.append(conn)
            asyncio.get_event_loop().create_task(self._wakeup())

    def __del__(self):
        self.close()
        asyncio.get_event_loop().create_task(self.wait_closed())

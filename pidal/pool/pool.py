import asyncio
import collections
import warnings

from typing import Set, Dict, Any

import async_timeout

from typing import Optional

from pidal.pool.client.connection import Connection


class Pool(asyncio.AbstractServer):

    def __init__(self,
                 minsize: int,
                 maxsize: int,
                 timeout: int = 10,
                 recycle: int = 0,
                 **kwargs):
        self.minsize: int = minsize
        self.maxsize: int = maxsize
        self.timeout: int = timeout
        self.recycle: int = recycle
        self.kwargs: Dict[str, Any] = kwargs

        self.loop: asyncio.AbstractEventLoop  = asyncio.get_running_loop()

        self._acquiring: int = 0
        self._free = collections.deque(maxlen=maxsize or None)
        self._cond = asyncio.Condition()
        self._used: Set[Connection] = set()
        self._terminated: Set[Connection] = set()
        self._closing: bool = False
        self._closed: bool = False

    @classmethod
    async def new(cls, minsize: int, maxsize: int, timeout=10, **kwargs):
        p = cls(minsize, maxsize, timeout, **kwargs)
        if p.minsize > 0:
            await p.fill_free()
        return p

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
                conn = await connect(
                    self._dsn, timeout=self._timeout,
                    enable_json=self._enable_json,
                    enable_hstore=self._enable_hstore,
                    enable_uuid=self._enable_uuid,
                    echo=self._echo,
                    **self._conn_kwargs)
                if self._on_connect is not None:
                    await self._on_connect(conn)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return

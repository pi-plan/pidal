import time
from typing import Optional

import aiomysql

from aiomysql.connection import MySQLResult
from pymysql.err import MySQLError

import pidal.node.result as result

from pidal.node.platform.dsn import DSN
from pidal.node.connection import Connection


class AIOMySQL(Connection):

    @classmethod
    def new(cls, dsn: DSN) -> Connection:
        c = cls(dsn)
        return c

    def __init__(self, dsn: DSN):
        self.dsn = dsn
        self.max_idle_time = dsn.max_idle_time
        self._conn: Optional[aiomysql.Connection] = None

        self._closed = False  # connect close flag
        self._last_executed = None
        self._result: Optional[MySQLResult] = None

    async def connect(self):

        if not self._closed:
            self.close()
        self._conn = await aiomysql.connect(**self.dsn.get_args())
        self._last_use_time = time.time()
        self._closed = False

    async def _ensure_connected(self):
        if self.max_idle_time and (time.time() - self._last_use_time
                                   > self.max_idle_time):
            await self.connect()
        self._last_use_time = time.time()

    async def begin(self):
        await self._ensure_connected()
        await self._conn.begin()

    async def commit(self):
        await self._ensure_connected()
        await self._conn.commit()

    async def rollback(self):
        await self._ensure_connected()
        await self._conn.rollback()

    async def query(self, sql) -> result.Result:
        try:
            await self._ensure_connected()
            await self._conn.query(sql)
            self._result = self._conn._result
            return self._read_result()
        except MySQLError as e:
            return result.Error(e.args[0], e.args[1])

    async def execute(self, sql) -> result.Result:
        return await self.query(sql)

    def _read_result(self) -> result.Result:
        if self._result.description is None:
            r = result.OK(self._result.affected_rows,  # type: ignore
                          self._result.insert_id,  # type: ignore
                          self._result.server_status,  # type: ignore
                          self._result.warning_count,  # type: ignore
                          self._result.message,  # type: ignore
                          self._result.has_next)  # type: ignore
            return r

        descriptions = []
        for i in self._result.description:
            desc = result.ResultDescription(
                        i.catalog,
                        i.db,
                        i.table_name,
                        i.org_table,
                        i.name,
                        i.org_name,
                        i.charsetnr,
                        i.length,
                        i.type_code,
                        i.flags,
                        i.scale)
            descriptions.append(desc)
        return result.ResultSet(self._result.field_count, descriptions,
                                list(self._result.rows))  # type: ignore

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._closed = True

    def is_closed(self) -> bool:
        return self._closed

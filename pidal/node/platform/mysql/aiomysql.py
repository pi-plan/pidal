import time
import traceback

from typing import Optional, Union

import aiomysql

from aiomysql.connection import MySQLResult
from aiomysql.cursors import DictCursor
from pymysql.err import MySQLError
from pymysql.constants import CLIENT

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

        self.closed = False  # connect close flag
        self._last_executed = None
        self.last_usage = 0
        self._result: Optional[MySQLResult] = None

    async def connect(self):
        client_flag = CLIENT.MULTI_STATEMENTS | CLIENT.MULTI_RESULTS
        if not self.closed:
            self.close()
        self._conn = await aiomysql.connect(**self.dsn.get_args(),
                                            client_flag=client_flag,
                                            cursorclass=DictCursor)
        await self._conn.query("set @@session.autocommit=0;")
        self._last_use_time = time.time()
        self.closed = False

    async def _ensure_connected(self):
        if self.max_idle_time and (time.time() - self._last_use_time
                                   > self.max_idle_time):
            await self.connect()
        self.last_usage = time.time()

    async def begin(self):
        await self._ensure_connected()
        await self._conn.begin()

    async def commit(self):
        await self._ensure_connected()
        await self._conn.commit()

    async def rollback(self):
        await self._ensure_connected()
        await self._conn.rollback()

    async def batch(self, sql) -> Union[DictCursor, result.Error]:
        try:
            await self._ensure_connected()
            cur = await self._conn.cursor()
            await cur.execute(sql)
            self._result = cur._result
            return cur
        except MySQLError as e:
            return result.Error(10003, str(e))

    async def query(self, sql) -> result.Result:
        try:
            await self._ensure_connected()
            await self._conn.query(sql)
            self._result = self._conn._result
            return self.read_result()
        except MySQLError as e:
            return result.Error(e.args[0], e.args[1])

    async def execute(self, sql) -> result.Result:
        return await self.query(sql)

    def read_result(self, _result: Optional[MySQLResult] = None) -> \
            result.Result:
        if _result is None:
            _result = self._result
        if _result.description is None:
            r = result.OK(_result.affected_rows,  # type: ignore
                          _result.insert_id,  # type: ignore
                          _result.server_status,  # type: ignore
                          _result.warning_count,  # type: ignore
                          _result.message,  # type: ignore
                          _result.has_next)  # type: ignore
            return r

        descriptions = []
        for i in _result.fields:
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
        return result.ResultSet(_result.field_count, descriptions,
                                list(_result.rows))  # type: ignore

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self.closed = True

    def is_closed(self) -> bool:
        return self.closed

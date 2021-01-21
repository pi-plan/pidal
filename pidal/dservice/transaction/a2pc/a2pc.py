import asyncio
from pidal.constant.db import TransStatus

from typing import Optional

import pidal.node.result as result

from pidal.dservice.backend.backend_manager import BackendManager
from pidal.dservice.database.database import Database
from pidal.dservice.sqlparse.paser import DML, Delete, Insert, SQL, Select, TCL, Update
from pidal.dservice.transaction.trans import Trans
from pidal.lib.snowflake import generator as snowflake


class A2PC(Trans):

    @classmethod
    def new(cls, db: Database, *node: str) -> 'A2PC':
        return cls(db, *node)

    def __init__(self, db: Database, *nodes: str):
        self.db = db
        self.nodes = nodes

        self.xid = next(snowflake)
        self.backend_manager = BackendManager.get_instance()
        self.status = TransStatus.INIT

    def get_status(self) -> TransStatus:
        return self.status

    async def begin(self, sql: TCL) -> Optional[result.Result]:
        self.status = TransStatus.BEGINNING
        # TODO 支持统一获取事务 ID
        self.status = TransStatus.ACTIVE

    async def commit(self, sql: TCL) -> Optional[result.Result]:
        pass

    async def rollback(self, sql: TCL) -> Optional[result.Result]:
        pass

    async def execute_dml(self, sql: DML) -> result.Result:
        if not sql.table:
            return await self.execute_other(sql)
        if isinstance(sql, Select):
            return await self.execute_select(sql)
        elif isinstance(sql, Insert):
            return await self.execute_insert(sql)
        elif isinstance(sql, Update):
            return await self.execute_update(sql)
        elif isinstance(sql, Delete):
            return await self.execute_delete(sql)
        else:
            return await self.execute_other(sql)

    async def execute_select(self, sql: Select):
        table = self.db.get_table(str(sql.table))
        if not sql.is_for_update:
            return table.execute_dml(sql, self.xid)
        # 给数据上锁
        if not sql.raw_where:
            raise Exception("select must where.")
        for i in table.

    async def execute_insert(self, sql: Insert):
        pass

    async def execute_update(self, sql: Update):
        pass

    async def execute_delete(self, sql: Delete):
        pass

    async def execute_other(self, sql: SQL) -> result.Result:
        return await self.db.execute_other(sql)

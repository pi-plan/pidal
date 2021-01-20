import asyncio
from pidal.constant.db import TransStatus

from typing import Optional

import pidal.node.result as result

from pidal.dservice.backend.backend_manager import BackendManager
from pidal.dservice.database.database import Database
from pidal.dservice.sqlparse.paser import DML, SQL, Select, TCL
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
        table = self.db.get_table(str(sql.table))
        nodes = table.get_node(sql)
        # 查询只需要一个就可以， Table 负责筛选出合适的node
        if isinstance(sql, Select):
            nodes = nodes[:1]
        return await table.execute_dml(sql, self.xid)

    async def execute_other(self, sql: SQL) -> result.Result:
        return await self.db.execute_other(sql)

import asyncio
from pidal.dservice.transaction.a2pc.client.client import A2PClient
from pidal.constant.db import TransStatus

from typing import Dict, Optional

import pidal.node.result as result

from pidal.dservice.backend.backend_manager import BackendManager
from pidal.dservice.database.database import Database
from pidal.dservice.sqlparse.paser import DML, DMLW, Delete, Insert, SQL, Select,\
        TCL, Update
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

        self.a2pc_client = A2PClient.get_instance()
        self._reundo_log: Dict[Dict[str]] = {}

    def get_status(self) -> TransStatus:
        return self.status

    async def begin(self, sql: TCL) -> Optional[result.Result]:
        self.status = TransStatus.BEGINNING
        # TODO 支持统一获取事务 ID
        self.status = TransStatus.ACTIVE

    async def commit(self, sql: TCL) -> Optional[result.Result]:
        r = await self.a2pc_client.commit(self.xid)
        if r.status == 0:
            return result.OK(0, 0, 0, 0, "", False)
        else:
            return result.Error(r.status, r.msg)

    async def rollback(self, sql: TCL) -> Optional[result.Result]:
        r = await self.a2pc_client.rollback(self.xid)
        if r.status == 0:
            return result.OK(0, 0, 0, 0, "", False)
        else:
            return result.Error(r.status, r.msg)

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

    async def execute_select(self, sql: Select) -> result.Result:
        table = self.db.get_table(str(sql.table))
        if not sql.is_for_update:
            return await table.execute_dml(sql, self.xid)
        # 给数据上锁
        if not sql.raw_where:
            raise Exception("select must where.")

        lock_keys = {}
        node = table.get_node(sql)
        for i in table.get_lock_columns():
            if i not in sql.raw_where.keys():
                raise Exception("lock data need [{}] column.".format(i))
            lock_keys[i] = sql.raw_where[i]
        lock = await self.a2pc_client.acquire_lock(self.xid, node[0].node,
                                                   table.get_name(), lock_keys,
                                                   str(sql.raw))
        if lock.status != 0:
            raise Exception("{} acquire lock fail: {}", str(sql.raw), lock.msg)

        return await table.execute_dml(sql, self.xid)

    async def execute_update(self, sql: Update) -> result.Result:
        table = self.db.get_table(str(sql.table))
        if not sql.raw_where:
            raise Exception("select must where.")

        lock_keys = {}
        node = table.get_node(sql)
        for i in table.get_lock_columns():
            if i not in sql.raw_where.keys():
                raise Exception("lock data need [{}] column.".format(i))
            lock_keys[i] = sql.raw_where[i]

        undo_log = {}
        # 先计算好数据在上锁。减少锁定时间。
        lock = await self.a2pc_client.acquire_lock(self.xid, node[0].node,
                                                   table.get_name(), lock_keys,
                                                   str(sql.raw))
        if lock.status != 0:
            raise Exception("{} acquire lock fail: {}", str(sql.raw), lock.msg)

    def _get_update_undo_log(sql: Update):

    async def execute_insert(self, sql: Insert) -> result.Result:
        pass

    async def execute_delete(self, sql: Delete) -> result.Result:
        pass

    async def execute_other(self, sql: SQL) -> result.Result:
        return await self.db.execute_other(sql)

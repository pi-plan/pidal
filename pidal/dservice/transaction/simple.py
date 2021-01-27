import asyncio
import traceback

from typing import Optional

import pidal.node.result as result

from pidal.logging import logger
from pidal.constant.db import TransStatus
from pidal.dservice.backend.backend_manager import BackendManager
from pidal.dservice.database.database import Database
from pidal.dservice.sqlparse.paser import DML, SQL, Select, TCL
from pidal.dservice.transaction.trans import Trans
from pidal.lib.snowflake import generator as snowflake


class Simple(Trans):
    """
    简单的事务模型，使用语法
    start transaction node1 node2
    Q：为什么需要 指定 node？
    A：因为是使用 backend 自身的本地事务机制，所以如果能提前知道用那些 node 最好。
    """

    @classmethod
    def new(cls, db: Database, *node: str) -> 'Simple':
        return cls(db, *node)

    def __init__(self, db: Database, *nodes: str):
        self.db = db
        self.nodes = set(nodes)

        self.xid = next(snowflake)
        self.backend_manager = BackendManager.get_instance()
        self.status = TransStatus.INIT

    def get_status(self) -> TransStatus:
        return self.status

    async def begin(self, sql: TCL) -> Optional[result.Result]:
        self.status = TransStatus.BEGINNING
        if self.nodes:
            g = []
            for i in self.nodes:
                g.append(self._begin(i))
            await asyncio.gather(*g)
        self.status = TransStatus.ACTIVE

    async def _begin(self, node: str):
        b = await self.backend_manager.get_backend(node, self.xid)
        try:
            await b.begin()
        except Exception as e:
            # TODO 针对异常进行不同的处理
            raise e

    async def commit(self, sql: TCL) -> Optional[result.Result]:
        self.status = TransStatus.COMMITING
        if self.nodes:
            g = []
            for i in self.nodes:
                g.append(self._commit(i))
            await asyncio.gather(*g)
        self.backend_manager.free_trans(self.xid)
        self.status = TransStatus.END

    async def _commit(self, node: str):
        b = await self.backend_manager.get_backend(node, self.xid)
        try:
            await b.commit()
        except Exception as e:
            # TODO 针对异常进行不同的处理
            raise e

    async def rollback(self, sql: TCL) -> Optional[result.Result]:
        self.status = TransStatus.ROLLBACKING
        if self.nodes:
            g = []
            for i in self.nodes:
                g.append(self._rollback(i))
            await asyncio.gather(*g)
        self.backend_manager.free_trans(self.xid)
        self.status = TransStatus.END

    async def _rollback(self, node: str):
        logger.debug("xid: {} rollback".format(self.xid))
        b = await self.backend_manager.get_backend(node, self.xid)
        try:
            await b.rollback()
        except Exception as e:
            # TODO 针对异常进行不同的处理
            raise e

    async def execute_dml(self, sql: DML) -> result.Result:
        logger.debug("xid: {} execute_dml".format(self.xid))
        if not sql.table:
            return await self.execute_other(sql)
        table = self.db.get_table(str(sql.table))
        nodes = table.get_node(sql)
        # 查询只需要一个就可以， Table 负责筛选出合适的node
        if isinstance(sql, Select):
            nodes = nodes[:1]
        for i in nodes:
            if i not in self.nodes:
                await self._begin(i.node)
                self.nodes.add(i.node)
        r = await table.execute_dml(sql, self.xid)
        return r

    async def execute_other(self, sql: SQL) -> result.Result:
        return await self.db.execute_other(sql)

    async def close(self):
        if self.status is not TransStatus.END:
            await self.rollback(None)
        self.backend_manager.free_trans(self.xid)
        logger.debug("xid: {} is closed".format(self.xid))
        for i in self.backend_manager.backends.keys():
            logger.debug(self.backend_manager.backends[i]._used)

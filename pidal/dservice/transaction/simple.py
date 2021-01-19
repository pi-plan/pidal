import asyncio
from typing import Optional
from pidal.dservice.backend.backend_manager import BackendManager
from pidal.dservice.database.database import Database
import pidal.node.result as result

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
        self.nodes = nodes

        self.xid = next(snowflake)
        self.backend_manager = BackendManager.get_instance()

    async def begin(self, sql: TCL) -> Optional[result.Result]:
        if self.nodes:
            g = []
            for i in self.nodes:
                g.append(self._begin(i))
            await asyncio.gather(*g)

    async def _begin(self, node: str):
        b = await self.backend_manager.get_backend(node, self.xid)
        try:
            await b.begin()
        except Exception as e:
            # TODO 针对异常进行不同的处理
            raise e

    async def commit(self, sql: TCL) -> Optional[result.Result]:
        if self.nodes:
            g = []
            for i in self.nodes:
                g.append(self._commit(i))
            await asyncio.gather(*g)

    async def _commit(self, node: str):
        b = await self.backend_manager.get_backend(node, self.xid)
        try:
            await b.begin()
        except Exception as e:
            # TODO 针对异常进行不同的处理
            raise e

    async def rollback(self, sql: TCL) -> Optional[result.Result]:
        if self.nodes:
            g = []
            for i in self.nodes:
                g.append(self._rollback(i))
            await asyncio.gather(*g)

    async def _rollback(self, node: str):
        b = await self.backend_manager.get_backend(node, self.xid)
        try:
            await b.begin()
        except Exception as e:
            # TODO 针对异常进行不同的处理
            raise e

    async def execute_dml(self, sql: DML) -> result.Result:
        if not sql.table:
            return await self.execute_other(sql)
        table = self.db.get_table(str(sql.table))
        nodes = table.get_node(sql)
        # 查询只需要一个就可以， Table 负责筛选出合适的node
        if isinstance(sql, Select):
            nodes = nodes[:1]
        for i in nodes:
            if i not in self.nodes:
                await self._begin(i)
        return await table.execute_dml(sql, self.xid)

    async def execute_other(self, sql: SQL) -> result.Result:
        return await self.db.execute_other(sql)

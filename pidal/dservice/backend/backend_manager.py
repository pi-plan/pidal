from pidal.meta.model import DBNode
from pidal.node.pool import Pool
from typing import Dict, Optional

from pidal.node.connection import Connection


class BackendManager(object):
    _instance: Optional['BackendManager'] = None

    @classmethod
    def new(cls) -> 'BackendManager':
        if cls._instance:
            return cls._instance
        c = cls()
        cls._instance = c
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'BackendManager':
        if not cls._instance:
            raise Exception("Not yet initialized")
        return cls._instance

    def __init__(self):
        # 事务中使用到的 node 保持
        self.trans: Dict[int, Dict[str, Connection]] = {}
        self.backends: Dict[str, Pool] = {}

    def add_backend(self, node: DBNode):
        if node.name in self.backends:
            return
        self.backends[node.name] = Pool(node.minimum_pool_size,
                                        node.maximum_pool_size,
                                        node.dsn,
                                        node.acquire_timeout,
                                        node.wait_time)

    async def get_backend(self, node: str, trans_id: int = 0) -> Connection:
        if trans_id:
            return await self.get_backend_by_trans(node, trans_id)
        return await self._acquiring_conn(node)

    async def _acquiring_conn(self, node: str) -> Connection:
        pool = self.backends.get(node, None)
        if not pool:
            raise Exception("unkonwn [{}] backend node .".format(node))

        return await pool.acquire()

    def release(self, node: str, conn: Connection):
        self.backends.get(node).release(conn)

    async def get_backend_by_trans(self, node: str,
                                   trans_id: int) -> Connection:
        trans = self.trans.get(trans_id, None)
        if not trans:
            trans = {}
            self.trans[trans_id] = trans
        if node in trans.keys():
            return trans[node]
        conn = await self._acquiring_conn(node)
        trans[node] = conn
        return conn

    def free_trans(self, trans_id: int):
        trans = self.trans.get(trans_id)
        if not trans:
            return
        for node, conn in trans.items():
            self.backends[node].release(conn)
        del(self.trans[trans_id])

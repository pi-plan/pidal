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
    def get_backend(self, node: str, trans_id: int = 0) -> Connection:
        pass

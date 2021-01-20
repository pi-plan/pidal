from typing import List

from pidal.dservice.backend.backend_manager import BackendManager
from pidal.lib.algorithms.factory import Factory as algorithms
from pidal.dservice.table.table import Table
from pidal.dservice.sqlparse.paser import DML, DMLW
from pidal.constant.db import DBTableType
from pidal.constant.common import RuleStatus
from pidal.meta.model import DBTable, DBTableStrategy, DBTableStrategyBackend
from pidal.node.result import result
from pidal.dservice.zone_manager import ZoneManager


class Raw(Table):

    @classmethod
    def new(cls, zone_manager: ZoneManager, table_conf: DBTable) -> 'Raw':
        t = cls(zone_manager, table_conf)
        return t

    def __init__(self, zone_manager: ZoneManager, table_conf: DBTable):
        self.zone_manager = zone_manager

        self.type = DBTableType.SHARDING
        self.name: str = table_conf.name
        self.status: RuleStatus = table_conf.status
        self.zskeys = table_conf.zskeys
        self.zs_algorithm = algorithms.new(table_conf.zs_algorithm)
        self.zs_algorithm_args = table_conf.zs_algorithm_args
        self.backend_manager = BackendManager.get_instance()
        if not table_conf.strategies or len(table_conf.strategies) != 1:
            raise Exception("Raw table need one strategy.")
        self._parse_strategies(table_conf.strategies[0])

    def _parse_strategies(self, strategy: DBTableStrategy):
        if not strategy.backends or len(strategy.backends) != 1:
            raise Exception("Raw table need one backends.")
        for i in strategy.backends:
            self.backend = i

    def get_name(self) -> str:
        return self.name

    def get_type(self) -> DBTableType:
        return self.type

    def get_status(self) -> RuleStatus:
        return self.status

    async def execute_dml(self, sql: DML, trans_id: int = 0) -> result.Result:
        node = self.get_node(sql)[0]
        backend = await self.backend_manager.get_backend(node.node, trans_id)
        if isinstance(sql, DMLW):
            sql.add_pidal(1)  # TODO 管理隐藏字段
        result = await backend.query(sql)
        return result

    def get_node(self, sql: DML) -> List[DBTableStrategyBackend]:
        if not self.backend:
            raise Exception("can not get backend.")
        return [self.backend]

from typing import Dict, List

from pidal.dservice.backend.backend_manager import BackendManager
from pidal.node.result import result
from pidal.lib.algorithms.factory import Factory as algorithms
from pidal.dservice.table.table import Table
from pidal.dservice.sqlparse.paser import DML
from pidal.constant.db import DBTableType
from pidal.constant.common import RuleStatus
from pidal.meta.model import DBTable, DBTableStrategy, DBTableStrategyBackend
from pidal.dservice.zone_manager import ZoneManager


class Sharding(Table):

    @classmethod
    def new(cls, zone_manager: ZoneManager, table_conf: DBTable) -> 'Sharding':
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
            raise Exception("Sharding table need one strategy.")
        self._parse_strategies(table_conf.strategies[0])

    def _parse_strategies(self, strategy: DBTableStrategy):
        if not strategy.algorithm:
            raise Exception("Sharding table need algorithm.")
        if not strategy.sharding_columns:
            raise Exception("Sharding table need sharding_columns.")
        if not strategy.backends:
            raise Exception("Sharding table need backends.")
        self.sharding_columns = strategy.sharding_columns
        self.sharding_algorithm = algorithms.new(strategy.algorithm)
        self.sharding_algorithm_args = strategy.algorithm_args
        self.backends: Dict[int, DBTableStrategyBackend] = {}
        for i in strategy.backends:
            self.backends[i.number] = i  # type: ignore

    def get_name(self) -> str:
        return self.name

    def get_type(self) -> DBTableType:
        return self.type

    def get_status(self) -> RuleStatus:
        return self.status

    async def execute_dml(self, sql: DML, trans_id: int = 0) -> result.Result:
        node = self.get_node(sql)
        backend = await self.backend_manager.get_backend(node[0], trans_id)
        result = await backend.query(sql)
        return result

    def get_node(self, sql: DML) -> List[str]:
        if not sql.table or not sql.column:
            raise Exception(
                    "SQL needs to contain the sharding fields[{}].".format(
                        ",".join(self.sharding_columns)))
        args = []
        if self.sharding_algorithm_args:
            args = self.sharding_algorithm_args

        for i in self.sharding_columns:
            cv = sql.column.get(i, None)
            if not cv:
                raise Exception(
                        "SQL needs to contain the sharding fields[{}].".format(
                            i))
            args.append(cv)

        sid = self.sharding_algorithm(*args)
        node = self.backends.get(sid, None)
        if not node:
            raise Exception("can not get backend.")
        return [node.node]

from typing import List

from pidal.lib.algorithms.factory import Factory as algorithms
from pidal.dservice.table.table import Table
from pidal.dservice.sqlparse.paser import DML
from pidal.constant.db import DBTableType
from pidal.constant.common import RuleStatus
from pidal.meta.model import DBTable, DBTableStrategy
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
        if not table_conf.strategies or len(table_conf.strategies) != 1:
            raise Exception("Raw table need one strategy.")
        self._parse_strategies(table_conf.strategies[0])

    def _parse_strategies(self, strategy: DBTableStrategy):
        if not strategy.backends or len(strategy.backends) != 1:
            raise Exception("Raw table need one backends.")
        for i in strategy.backends:
            self.backend = i.node

    def get_node(self, sql: DML) -> List[str]:
        if not self.backend:
            raise Exception("can not get backend.")
        return [self.backend]

import asyncio
from typing import Dict, List

from pidal.dservice.table.tools import Tools
from pidal.dservice.backend.backend_manager import BackendManager
from pidal.node.result import result
from pidal.lib.algorithms.factory import Factory as algorithms
from pidal.dservice.table.table import Table
from pidal.dservice.sqlparse.paser import DML, DMLW
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
        self.lock_key = table_conf.lock_key
        self.backend_manager = BackendManager.get_instance()
        if not table_conf.strategies or len(table_conf.strategies) != 1:
            raise Exception("Sharding table need one strategy.")
        self._parse_strategies(table_conf.strategies[0])
        self._parse_table_scheme()

    def _parse_table_scheme(self):
        loop = asyncio.get_event_loop()
        node = list(self.backends.values())[0]
        backend = loop.run_until_complete(
                self.backend_manager.get_backend(node.node))

        _table = node.prefix + str(node.number)
        self.column_default = loop.run_until_complete(
                Tools.get_column_default(backend, _table))

        self.lock_columns = loop.run_until_complete(
                Tools.get_lock_columns(backend, _table, self.lock_key))
        self.backend_manager.release(node.node, backend)

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
        node = self.get_node(sql)[0]
        backend = await self.backend_manager.get_backend(node.node, trans_id)
        sql.modify_table(node.prefix + str(node.number))
        if isinstance(sql, DMLW):
            sql.add_pidal(1)  # TODO 管理隐藏字段
        result = await backend.query(sql)
        return result

    def get_node(self, sql: DML) -> List[DBTableStrategyBackend]:
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
        return [node]

    def get_lock_columns(self) -> List[str]:
        return self.lock_columns

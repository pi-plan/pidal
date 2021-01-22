import asyncio
from typing import Dict, List

from pidal.dservice.table.tools import Tools
from pidal.dservice.backend.backend_manager import BackendManager
from pidal.lib.algorithms.factory import Factory as algorithms
from pidal.dservice.table.table import Table
from pidal.dservice.sqlparse.paser import DML, DMLW, Select
from pidal.constant.db import DBTableType
from pidal.constant.common import RuleStatus
from pidal.meta.model import DBTable, DBTableStrategy, DBTableStrategyBackend
from pidal.dservice.zone_manager import ZoneManager
from pidal.node.result import result


class DoubleSharding(Table):

    @classmethod
    def new(cls, zone_manager: ZoneManager, table_conf: DBTable) -> \
            'DoubleSharding':
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
        if not table_conf.strategies or len(table_conf.strategies) != 2:
            raise Exception("Sharding table need two strategy.")
        self._parse_strategies(table_conf.strategies)
        self._parse_table_scheme()

    def _parse_table_scheme(self):
        loop = asyncio.get_event_loop()
        node = list(self.backends[0].values())[0]
        backend = loop.run_until_complete(
                self.backend_manager.get_backend(node.node))

        _table = node.prefix + str(node.number)
        self.column_default = loop.run_until_complete(
                Tools.get_column_default(backend, _table))

        self.lock_columns = loop.run_until_complete(
                Tools.get_lock_columns(backend, _table, self.lock_key))

    def _parse_strategies(self, strategies: List[DBTableStrategy]):
        self.sharding_columns = []
        self.sharding_algorithm = []
        self.sharding_algorithm_args = []
        self.backends: List[Dict[int, DBTableStrategyBackend]] = []
        for index, strategy in enumerate(strategies):
            if not strategy.algorithm:
                raise Exception("Sharding table need algorithm.")
            if not strategy.sharding_columns:
                raise Exception("Sharding table need sharding_columns.")
            if not strategy.backends:
                raise Exception("Sharding table need backends.")
            self.sharding_columns.append(strategy.sharding_columns)
            self.sharding_algorithm.append(algorithms.new(strategy.algorithm))
            self.sharding_algorithm_args.append(strategy.algorithm_args)
            self.backends.append({})
            for i in strategy.backends:
                self.backends[index][i.number] = i  # type: ignore

    def get_name(self) -> str:
        return self.name

    def get_type(self) -> DBTableType:
        return self.type

    def get_status(self) -> RuleStatus:
        return self.status

    async def execute_dml(self, sql: DML, trans_id: int = 0) -> result.Result:
        nodes = self.get_node(sql)
        if isinstance(sql, Select):
            nodes = nodes[:1]
        g = []
        for node in nodes:
            g.append(self._execute_dml(node, sql, trans_id))
        r = await asyncio.gather(*g)
        # TODO 两个之间的异常处理
        if len(r) < 2:
            return r[0]
        else:
            for ri in r:
                if not isinstance(ri, result.OK):
                    return ri
            return r[0]

    async def _execute_dml(self, node: DBTableStrategyBackend, sql: DML,
                           trans_id: int = 0) -> result.Result:
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
                        ",".join(self.sharding_columns[0])))
        result = []
        for i in range(0, 2):
            args = []
            if self.sharding_algorithm_args[i]:
                args = self.sharding_algorithm_args[i]

            for i in self.sharding_columns[i]:
                cv = sql.column.get(i, None)
                if not cv:
                    continue
                args.append(cv)

            sid = self.sharding_algorithm[i](*args)
            node = self.backends[i].get(sid, None)
            if not node:
                raise Exception("can not get backend.")
            result.append(node)
        if not result:
            raise Exception(
                    "SQL needs to contain the sharding fields[{}].".format(
                        ",".join(self.sharding_algorithm[0])))
        return result

    def get_lock_columns(self) -> List[str]:
        return self.lock_columns

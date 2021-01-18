from pidal.dservice.sqlparse.paser import DML, SQL
from typing import Dict, List, Optional

import pidal.node.result as result
from pidal.dservice.backend.backend_manager import BackendManager
from pidal.dservice.table.table import Table
from pidal.dservice.zone_manager import ZoneManager
from pidal.meta.model import DBConfig


class Database(object):

    def __init__(self, zone_manager: ZoneManager, db_config: DBConfig):
        self.zone_manager = zone_manager
        self.db_config = db_config
        self.default_trans_mod = db_config.transaction_mod
        self.idle_in_transaction_session_timeout = \
            db_config.idle_in_transaction_session_timeout
        self.tables: Dict[str, Table] = {}
        self.create_tables()
        self.create_backends()

    def create_tables(self):
        for i in self.db_config.tables.values():
            table = Table(self.zone_manager, i)
            self.tables[table.name] = table

    def create_backends(self):
        manager = BackendManager.get_instance()
        for i in self.db_config.nodes.values():
            manager.add_backend(i)

    async def execute_command(self, execute: result.Execute) -> \
            Optional[List[result.Result]]:
        """ 处理非 Query 类型的command  """
        pass

    async def execute_dml(self, sql: DML) -> result.Result:
        pass

    async def execute_other(self, sql: SQL) -> result.Result:
        pass

    def get_table(self, t: str) -> Table:
        table = self.tables.get(t, None)
        if not table:
            raise Exception("not found table [{}]".format(t))
        return table

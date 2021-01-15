from typing import Dict
from pidal.dservice.table.table import Table
from pidal.dservice.zone_manager import ZoneManager
from pidal.meta.model import DBConfig


class Database(object):

    def __init__(self, zone_manager: ZoneManager, db_config: DBConfig):
        self.zone_manager = zone_manager
        self.db_config = db_config
        self.tables: Dict[str, Table] = {}
        self.create_tables()

    def create_tables(self):
        for i in self.db_config.tables.values():
            table = Table(self.zone_manager, i)
            self.tables[table.name] = table

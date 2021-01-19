from pidal.dservice.zone_manager import ZoneManager
from pidal.meta.model import DBTable
from pidal.dservice.table.table import Table
from pidal.dservice.table.double_sharding import DoubleSharding
from pidal.dservice.table.sharding import Sharding
from pidal.dservice.table.raw import Raw
from pidal.constant.db import DBTableType


class TableFactory(object):
    tables = {
            DBTableType.RAW: Raw,
            DBTableType.SHARDING: Sharding,
            DBTableType.DOUBLE_SHARDING: DoubleSharding,
            }

    @classmethod
    def new(cls, t: DBTableType, zone_manager: ZoneManager,
            table_conf: DBTable) -> Table:
        tc = cls.tables.get(t, None)
        if not tc:
            raise Exception("unkonwn table type.")
        return tc.new(zone_manager, table_conf)

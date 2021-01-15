from pidal.constant.db import DBTableType
from pidal.constant.common import RuleStatus
from pidal.meta.model import DBTable
from pidal.dservice.zone_manager import ZoneManager


class Table(object):

    def __init__(self, zone_manager: ZoneManager, table_conf: DBTable):
        self.zone_manager = zone_manager

        self.type: DBTableType = table_conf.type
        self.name: str = table_conf.name
        self.status: RuleStatus = table_conf.status
        self.zskeys = table_conf.zskeys
        self.zs_algorithm = table_conf.zs_algorithm

    def query(self, trans_id: int, sql):
        """ 如果是在事务中需要传入 trans_id """
        pass

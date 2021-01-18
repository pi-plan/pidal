import abc
from typing import List

from pidal.dservice.sqlparse.paser import DML
from pidal.constant.db import DBTableType
from pidal.constant.common import RuleStatus
from pidal.meta.model import DBTable
from pidal.dservice.zone_manager import ZoneManager


class Table(metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractclassmethod
    def new(self, zone_manager: ZoneManager, table_conf: DBTable) -> 'Table':
        pass

    @abc.abstractmethod
    def query(self, sql, trans_id: int = 0):
        pass

    @abc.abstractmethod
    def get_node(self, sql: DML) -> List[str]:
        pass

    @abc.abstractmethod
    def get_type(self) -> DBTableType:
        pass

    @abc.abstractmethod
    def get_status(self) -> RuleStatus:
        pass

import abc
from typing import List

from pidal.node.result import result
from pidal.dservice.sqlparse.paser import DML
from pidal.constant.db import DBTableType
from pidal.constant.common import RuleStatus
from pidal.meta.model import DBTable, DBTableStrategyBackend
from pidal.dservice.zone_manager import ZoneManager


class Table(metaclass=abc.ABCMeta):
    # TODO 启动后，分析表的主键和唯一性约束.

    @classmethod
    @abc.abstractclassmethod
    def new(cls, zone_manager: ZoneManager, table_conf: DBTable) -> 'Table':
        pass

    @abc.abstractmethod
    async def execute_dml(self, sql: DML, trans_id: int = 0) -> result.Result:
        """ 如果是在事务中需要传入 trans_id """
        pass

    @abc.abstractmethod
    def get_node(self, sql: DML) -> List[DBTableStrategyBackend]:
        pass

    @abc.abstractmethod
    def get_name(self) -> str:
        pass

    @abc.abstractmethod
    def get_type(self) -> DBTableType:
        pass

    @abc.abstractmethod
    def get_status(self) -> RuleStatus:
        pass

    @abc.abstractclassmethod
    def get_lock_columns(self) -> List[str]:
        pass

import abc
from pidal.constant.db import TransStatus

from typing import Optional

import pidal.node.result as result

from pidal.dservice.sqlparse.paser import DML, SQL, TCL


class Trans(metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractclassmethod
    def new(cls) -> 'Trans':  # type: ignore
        pass

    @abc.abstractmethod
    async def begin(self, sql: TCL) -> Optional[result.Result]:
        pass

    @abc.abstractmethod
    async def commit(self, sql: TCL) -> Optional[result.Result]:
        pass

    @abc.abstractmethod
    async def rollback(self, sql: TCL) -> Optional[result.Result]:
        pass

    @abc.abstractmethod
    async def execute_dml(self, sql: DML) -> result.Result:
        pass

    @abc.abstractmethod
    async def execute_other(self, sql: SQL) -> result.Result:
        pass

    @abc.abstractmethod
    def get_status(self) -> TransStatus:
        pass

    @abc.abstractmethod
    async def close(self):
        pass

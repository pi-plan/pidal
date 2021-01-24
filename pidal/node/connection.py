import abc

import pidal.node.result as result

from pidal.node.platform.dsn import DSN


class Connection(abc.ABC):

    @classmethod
    @abc.abstractclassmethod
    def new(cls, dsn: DSN) -> 'Connection':
        pass

    @abc.abstractmethod
    async def connect(self):
        pass

    @abc.abstractmethod
    async def begin(self):
        pass

    @abc.abstractmethod
    async def commit(self):
        pass

    @abc.abstractmethod
    async def rollback(self):
        pass

    @abc.abstractmethod
    async def query(self, sql: str) -> result.Result:
        pass

    @abc.abstractmethod
    async def execute(self, sql: str) -> result.Result:
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def is_closed(self) -> bool:
        pass

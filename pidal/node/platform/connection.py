import abc

import pidal.protocol.backend as result


class Connection(abc.ABC):

    @classmethod
    @abc.abstractclassmethod
    async def new(cls, **kwargs) -> 'Connection':
        pass

    @abc.abstractmethod
    async def connect(self):
        pass

    @abc.abstractmethod
    async def begin(self) -> result.Result:
        pass

    @abc.abstractmethod
    async def commit(self) -> result.Result:
        pass

    @abc.abstractmethod
    async def rollback(self) -> result.Result:
        pass

    @abc.abstractmethod
    async def query(self, sql) -> result.Result:
        pass

    @abc.abstractmethod
    async def execute(self, sql) -> result.Result:
        pass

    @abc.abstractmethod
    def clean(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def is_closed(self) -> bool:
        pass

    @abc.abstractmethod
    async def get_last_executed(self) -> str:
        pass

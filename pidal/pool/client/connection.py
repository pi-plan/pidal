import abc

import pidal.protocol.result as result


class Connection(abc.ABC):

    @classmethod
    @abc.abstractclassmethod
    def new(cls, **connection_kwargs):
        pass

    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def begin(self) -> result.Result:
        pass

    @abc.abstractmethod
    def commit(self) -> result.Result:
        pass

    @abc.abstractmethod
    def rollback(self) -> result.Result:
        pass

    @abc.abstractmethod
    def query(self, sql) -> result.Result:
        pass

    @abc.abstractmethod
    def execute(self, sql) -> result.Result:
        pass

    @abc.abstractmethod
    def close(self, sql) -> result.Result:
        pass

    @abc.abstractmethod
    def get_last_executed(self) -> str:
        pass

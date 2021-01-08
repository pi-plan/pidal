import abc

from typing import List, Tuple

from pidal.protocol.result.description import Description


class Result(abc.ABC):

    @abc.abstractmethod
    def to_mysql(self):
        pass


class ResultSet(Result):

    def __init__(self):
        self.descriptions: List[Description] = []
        self.rows: List[Tuple[str]] = []


class Execute(Result):

    def __init__(self):
        self.sql: str = ''


class OK(Result):

    def __init__(self):
        self.affected_rows: int = 0
        self.insert_id: int = 0
        self.server_status: int = 0
        self.warning_count: int = 0
        self.message: str = ""
        self.has_next: bool = False


class EOF(Result):

    def __init__(self):
        self.server_status: int = 0
        self.warning_count: int = 0
        self.has_next: bool = False


class Error(Result):

    def __init__(self):
        self.error_code: int = 0
        self.sql_state: str = ''
        self.message: str = ''

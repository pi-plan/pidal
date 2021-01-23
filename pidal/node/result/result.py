import abc

from typing import Any, Dict, List, Tuple, Optional

from pidal.node.result.command import Command


class Result(abc.ABC):

    @abc.abstractmethod
    def to_mysql(self):
        pass


class ResultDescription(Result):

    def __init__(self,
                 catalog: Optional[str],
                 db: Optional[str],
                 table_name: Optional[str],
                 org_table: Optional[str],
                 name: Optional[str],
                 org_name: Optional[str],
                 charsetnr: int,
                 length: int,
                 type_code: int,
                 flags: int,
                 scale: int):
        self.catalog: Optional[str] = catalog
        self.db: Optional[str] = db
        self.table_name: Optional[str] = table_name
        self.org_table: Optional[str] = org_table
        self.name: Optional[str] = name
        self.org_name: Optional[str] = org_name
        self.charsetnr: int = charsetnr
        self.length: int = length
        self.type_code: int = type_code
        self.flags: int = flags
        self.scale: int = scale

    def to_mysql(self):
        pass


class ResultSet(Result):

    def __init__(self,
                 field_count: int,
                 descriptions: List[ResultDescription],
                 rows: List[Tuple[Optional[str]]]):
        self.field_count: int = field_count
        self.descriptions: List[ResultDescription] = descriptions
        self.rows: List[Tuple[Any]] = rows

    def to_dict(self) -> List[Dict[str, Any]]:
        columns = []
        for d in self.descriptions:
            columns.append(d.org_name if d.org_name else d.name)
        result = []
        for r in self.rows:
            row = {}
            for i, c in enumerate(r):
                row[columns[i]] = c
            result.append(row)
        return result

    def to_mysql(self):
        pass


class Execute(Result):

    def __init__(self, length: int, command: Command, args: bytes, query: str):
        self.length = length
        self.command: Command = command
        self.args: bytes = args
        self.query: str = query

    def to_mysql(self):
        pass


class OK(Result):

    def __init__(self, affected_rows: int, insert_id: int, server_status: int,
                 warning_count: int, message: str, has_next: bool):
        self.affected_rows: int = affected_rows
        self.insert_id: int = insert_id
        self.server_status: int = server_status
        self.warning_count: int = warning_count
        self.message: str = message
        self.has_next: bool = has_next

    def to_mysql(self):
        pass


class EOF(Result):

    def __init__(self):
        self.server_status: int = 0
        self.warning_count: int = 0
        self.has_next: bool = False

    def to_mysql(self):
        pass


class Error(Result):

    def __init__(self, error_code: int, message: str):
        self.error_code: int = error_code
        #  self.sql_state: int = sql_state  # 应该有 PiDAL 来决定，
        self.message: str = message

    def to_mysql(self):
        pass

import json

from typing import Any, Dict, Optional

from pidal.node.result import result
from pidal.dservice.transaction.a2pc.client.constant import A2PCOperation,\
        A2PCStatus
from pidal.dservice.transaction.a2pc.reundo.reundo_log import ReUnDoLog


class Unique(ReUnDoLog):

    @classmethod
    def new(cls, xid: int, lock_keys: Dict[str, Any], table: str):
        return cls(xid, lock_keys, table)

    def __init__(self, xid: int, lock_keys: Dict[str, Any], table: str):
        self.xid = xid
        self.table = table
        self.lock_keys = lock_keys
        self.context: Optional[str] = None
        self.operation: Optional[A2PCOperation] = None
        self.undo: Optional[Dict[str, Any]] = None
        self.redo: Optional[Dict[str, Any]] = None

    def set_undo(self, r: Optional[result.ResultSet]):
        if not r:
            return
        old_data = r.to_dict()
        if not old_data or len(old_data) != 1:
            raise Exception("data must unique.")

        if self.undo is None:
            self.undo = old_data[0]
        elif self.undo != old_data:
            raise Exception("local data record error.")

    def set_redo(self, v: Optional[Dict[str, Any]], operation: A2PCOperation):
        self.redo = v
        self.operation = operation

    def set_context(self, context: str):
        self.context = context

    def to_sql(self, status: A2PCStatus, client_id: str = "") -> Optional[str]:
        if not self.undo and not self.redo:
            return None
        sql = """
insert into reundo_log (`lock_key`, `xid`, `context`, `table`, `reundo_log`,
`status`, `client_id`) VALUES ({}, {}, {}, {}, {}. {}, {});
        """
        lock_keys = json.dumps(self.lock_keys)
        reundo_log = {
                "operation": self.operation,
                "undo": self.undo,
                "redo": self.redo
                }
        reundo_log_str = json.dumps(reundo_log)
        return sql.format(lock_keys, self.xid, self.context, self.table,
                          reundo_log_str.encode(), status, client_id)

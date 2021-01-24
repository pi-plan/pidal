from typing import Any, Dict, Optional

from pidal.node.result import result
from pidal.dservice.transaction.a2pc.client.constant import A2PCOperation,\
        A2PCStatus


class ReUnDoLog(object):

    @classmethod
    def new(cls, xid: int, lock_keys: Dict[str, Any], table: str):
        pass

    def set_undo(self, r: Optional[result.ResultSet]):
        pass

    def set_redo(self, v: Optional[Dict[str, Any]], operation: A2PCOperation):
        pass

    def set_context(self, context: str):
        pass

    def to_sql(self, status: A2PCStatus, client_id: str = "") -> Optional[str]:
        pass

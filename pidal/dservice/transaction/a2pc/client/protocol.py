import json
from typing import Any, Dict
from pidal.dservice.transaction.a2pc.client.constant import A2PCAction


class Protocol(object):

    @classmethod
    def new(cls, j: str):
        p = json.loads(j)
        action = A2PCAction(p["action"])
        o = cls(action)

        return o

    def __init__(self, action: A2PCAction):
        self.action = action
        self.xid: int
        self.node: str
        self.table: str
        self.lock_key: Dict[str, Any]
        self.context: Any
        self.client_id: str

    def parser(self, params: Dict[str, Any]):
        if self.action is A2PCAction.COMMIT:
            self.xid = params["xid"]
        elif self.action is A2PCAction.ROLLBACK:
            self.xid = params["xid"]
        elif self.action is A2PCAction.ACQUIRE_LOCK:
            self.xid = params["xid"]
            self.node = params["node"]
            self.table = params["table"]
            self.columns = params["columns"]

        if "context" in params.keys():
            self.context = params["context"]

        if "client_id" in params.keys():
            self.context = params["client_id"]

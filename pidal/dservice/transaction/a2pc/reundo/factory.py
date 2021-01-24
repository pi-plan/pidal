from typing import Any, Dict

from pidal.dservice.transaction.a2pc.reundo.unique import Unique
from pidal.dservice.transaction.a2pc.reundo.reundo_log import ReUnDoLog


class Factory(object):

    c_map = {
            "unqiue": Unique,
            }

    @classmethod
    def new(cls, type: str, xid: int, lock_keys: Dict[str, Any], table: str) \
            -> ReUnDoLog:
        c = cls.c_map.get(type, None)
        assert c
        return c(xid, lock_keys, table)

from pidal.constant.db import SessionStatus
import pidal.node.result as result

from pidal.dservice.database.database import Database


class DSession(object):
    def __init__(self, database: Database):
        self.db = database
        self.status = SessionStatus.SERVING

    def get_session_status(self) -> SessionStatus:
        return self.status

    async def query(self, execute: result.Execute) -> result.Result:
        pass

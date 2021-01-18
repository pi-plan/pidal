from pidal.dservice.transaction.factory import TransFactory
from typing import List, Optional

import pidal.node.result as result

from pidal.dservice.database.database import Database
from pidal.dservice.transaction.trans import Trans
from pidal.dservice.sqlparse.paser import Parser, DML, SQL, TCL, Other
from pidal.node.result.command import Command
from pidal.constant.db import SessionStatus


class DSession(object):
    def __init__(self, database: Database):
        self.db = database
        self.status = SessionStatus.SERVING

        self._trans: Optional[Trans] = None

    def get_session_status(self) -> SessionStatus:
        return self.status

    async def execute(self, execute: result.Execute) -> \
            Optional[List[result.Result]]:
        if execute.command is not Command.COM_QUERY:
            return await self._execute_command(execute)
        sqls = Parser.parse(execute.query)
        r = []
        for sql in sqls:
            if isinstance(sql, DML):
                r.append(await self._execute_dml(sql))
            elif isinstance(sql, TCL):
                r.append(await self._execute_trans(sql))
            else:
                r.append(await self._execute_other(sql))
        return r

    async def _execute_command(self, execute: result.Execute) -> \
            Optional[List[result.Result]]:
        """ 处理非 Query 类型的command  """
        return await self.db.execute_command(execute)

    async def _execute_other(self, sql: SQL) -> result.Result:
        if self._trans:
            return await self._trans.execute_other(sql)
        return await self.db.execute_other(sql)

    async def _execute_trans(self, sql: TCL) -> result.Result:
        # TODO 模拟 OK, Error 包
        if sql.is_start:
            if self._trans:
                raise Exception("Transactions cannot be nested.")
            self._trans = self._create_trans(sql.trans_args)
            return await self._trans.begin(sql)
        elif sql.is_commit:
            if self._trans:
                return await self._trans.commit(sql)
            else:
                pass  # TODO 幂等返回 OK 包
        elif sql.is_rollback:
            if self._trans:
                return await self._trans.rollback(sql)
            else:
                pass  # TODO 幂等返回 OK 包
        raise Exception("unkonwn transaction operation.")

    async def _execute_dml(self, sql: DML) -> result.Result:
        if self._trans:
            return await self._trans.execute_dml(sql)
        return await self.db.execute_dml(sql)

    def _create_trans(self, trans: Optional[List[str]]) -> Trans:
        args = []
        if trans:
            trans_mod = trans[0]
            if len(trans_mod) > 1:
                args = trans[1:]
        else:
            trans_mod = self.db.default_trans_mod
        return TransFactory.new(trans_mod, self.db, *args)

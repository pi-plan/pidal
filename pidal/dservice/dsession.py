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
        try:
            if sql.is_start:
                if self._trans:
                    raise Exception("Transactions cannot be nested.")
                self._trans = self._create_trans(sql.trans_args)
                await self._trans.begin(sql)
                return result.OK(0, 0, 0, 0, '', False)
            elif sql.is_commit:
                if self._trans:
                    await self._trans.commit(sql)
                    await self._trans.close()
                    self._trans = None
                return result.OK(0, 0, 0, 0, '', False)
            elif sql.is_rollback:
                if self._trans:
                    await self._trans.rollback(sql)
                    await self._trans.close()
                    self._trans = None
                return result.OK(0, 0, 0, 0, '', False)
            else:
                return result.Error(1000, "unknown what to do.")
        except Exception as e:
            await self._trans.close()
            self._trans = None
            return result.Error(1000, str(e))

    async def _execute_dml(self, sql: DML) -> result.Result:
        if self._trans:
            return await self._trans.execute_dml(sql)
        if not sql.has_table():
            return await self._execute_other(sql)
        table = self.db.get_table(str(sql.table))
        return await table.execute_dml(sql)

    def _create_trans(self, trans: Optional[List[str]]) -> Trans:
        args = []
        if trans:
            trans_mod = trans[0]
            if len(trans_mod) > 1:
                args = trans[1:]
        else:
            trans_mod = self.db.default_trans_mod
        return TransFactory.new(trans_mod, self.db, *args)

    async def close(self):
        if self._trans:
            await self._trans.close()

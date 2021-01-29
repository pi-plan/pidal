import asyncio
from pidal.node.connection import Connection

from typing import Any, Dict, List, Optional, Union

import sqlparse

import pidal.node.result as result

from pidal.logging import logger
from pidal.dservice.table.raw import Raw
from pidal.dservice.table.table import Table
from pidal.meta.model import DBTableStrategyBackend
from pidal.dservice.transaction.a2pc.client.constant import A2PCOperation,\
        A2PCStatus
from pidal.dservice.transaction.a2pc.client.client import A2PCResponse, A2PClient
from pidal.constant.db import TransStatus
from pidal.dservice.backend.backend_manager import BackendManager
from pidal.dservice.database.database import Database
from pidal.dservice.sqlparse.paser import DML, Delete, Insert, SQL, Select,\
        TCL, Update, DMLW
from pidal.dservice.transaction.trans import Trans
from pidal.dservice.transaction.a2pc.reundo.reundo_log import ReUnDoLog
from pidal.dservice.transaction.a2pc.reundo.factory import \
        Factory as ReUnFactory


class A2PC(Trans):

    @classmethod
    def new(cls, db: Database, *node: str) -> 'A2PC':
        return cls(db, *node)

    def __init__(self, db: Database, *nodes: str):
        self.db = db
        self.nodes = nodes

        self.xid = 0
        self.backend_manager = BackendManager.get_instance()
        self.status = TransStatus.INIT

        self.a2pc_client = A2PClient.get_instance()
        self._reundo_log: Dict[str, Dict[str, ReUnDoLog]] = {}

    def get_status(self) -> TransStatus:
        return self.status

    async def begin(self, sql: TCL) -> Optional[result.Result]:
        self.status = TransStatus.BEGINNING
        r = await self.a2pc_client.begin()
        if r.status == 0:
            self.xid = r.xid
        else:
            return result.Error(r.status, r.msg)
        self.status = TransStatus.ACTIVE

    async def commit(self, sql: TCL) -> Optional[result.Result]:
        r = await self.a2pc_client.commit(self.xid)
        if r.status == 0:
            self.status = TransStatus.END
            return result.OK(0, 0, 0, 0, "", False)
        else:
            return result.Error(r.status, r.msg)

    async def rollback(self, sql: TCL) -> Optional[result.Result]:
        r = await self.a2pc_client.rollback(self.xid)
        if r.status == 0:
            self.status = TransStatus.END
            return result.OK(0, 0, 0, 0, "", False)
        else:
            return result.Error(r.status, r.msg)

    async def execute_dml(self, sql: DML) -> result.Result:
        if not sql.table:
            return await self.execute_other(sql)
        if isinstance(sql, Select):
            return await self.execute_select(sql)
        elif isinstance(sql, Insert):
            return await self.execute_insert(sql)
        elif isinstance(sql, Update):
            return await self.execute_update(sql)
        elif isinstance(sql, Delete):
            return await self.execute_delete(sql)
        else:
            return await self.execute_other(sql)

    async def execute_select(self, sql: Select) -> result.Result:
        table = self.db.get_table(str(sql.table))
        if not sql.is_for_update:
            return await table.execute_dml(sql, self.xid)
        # 给数据上锁
        if not sql.raw_where:
            raise Exception("select must contain [where].")

        lock_keys = {}
        node = table.get_node(sql)
        for i in table.get_lock_columns():
            if i not in sql.raw_where.keys():
                raise Exception("lock data need [{}] column.".format(i))
            lock_keys[i] = sql.raw_where[i]
        lock = await self.a2pc_client.acquire_lock(self.xid, node[0].node,
                                                   str(sql.table), lock_keys,
                                                   str(sql.raw))
        if lock.status != 0:
            raise Exception("{} acquire lock fail: {}", str(sql.raw), lock.msg)

        r = await table.execute_dml(sql, self.xid)
        if isinstance(r, result.ResultSet):
            self._record_undo_data(table.get_name(), lock_keys, r)
        return r

    def _record_undo_data(self, table: str, lock_keys: Dict[str, Any],
                          old_raw: Optional[result.ResultSet]):
        table_logs = self._reundo_log.get(table, None)
        if not table_logs:
            table_logs = {}
            self._reundo_log[table] = table_logs
        log = table_logs.get(str(lock_keys), None)
        if not log:
            log = ReUnFactory.new("unique", self.xid, lock_keys, table)
            self._reundo_log[table][str(lock_keys)] = log
        log.set_undo(old_raw)

    async def execute_update(self, sql: Update) -> result.Result:
        table = self.db.get_table(str(sql.table))
        if not sql.raw_where:
            raise Exception("update must contain [where].")

        lock_keys = {}
        node = table.get_node(sql)
        for i in table.get_lock_columns():
            if i not in sql.raw_where.keys():
                raise Exception("lock data need [{}] column.".format(i))
            lock_keys[i] = sql.raw_where[i]

        error = await self._update_undo_log(lock_keys, sql)
        if error:
            return error  # type: ignore
        reundo_sql = self.reundo_log_generator(table.get_name(), lock_keys)
        if not reundo_sql:
            raise Exception("can not get redo undo log.")
        before_sql = "begin;" + reundo_sql
        c = []
        for i in node:
            c.append(self._execute_update(table, i, before_sql, sql))

        # 先计算好数据在上锁。减少锁定时间。
        lock_c = self.a2pc_client.acquire_lock(self.xid, node[0].node,
                                               str(sql.table), lock_keys,
                                               str(sql.raw))
        c.append(lock_c)
        r = await asyncio.gather(*c)
        lock = r[-1]
        r = r[:-1]
        ending_sql = "COMMIT"
        return_v = None
        if lock.status != 0:
            logger.warning("{} acquire lock fail: {}".format(
                             str(sql.raw), lock.msg))
            ending_sql = "ROLLBACK"
            return_v = result.Error(1000, "{} acquire lock fail: {}".format(
                             str(sql.raw), lock.msg))
        if any([isinstance(i, result.Error) for i in r]):
            errors = [i for i in r if isinstance(i, result.Error)]
            for i in errors:
                logger.warning("{} error with code[{}],msg: {}".format(
                                 str(sql.raw), i.error_code, i.message))
            ending_sql = "ROLLBACK"
            return_v = errors[0]
        c = []
        for i in node:
            c.append(self._execute_ending_sql(ending_sql, i))
        await asyncio.gather(*c)
        if ending_sql == "ROLLBACK":
            return return_v  # type: ignore
        return result.OK(1, 0, 2, 0, "", False)

    async def _execute_ending_sql(self, ending_sql: str,
                                  node: DBTableStrategyBackend):
        backend = await self.backend_manager.get_backend(node.node, self.xid)
        if ending_sql == "ROLLBACK":
            return await backend.rollback()
        return await backend.commit()

    async def _execute_dml(self, node: DBTableStrategyBackend,
                           before_sql: str, sql: DMLW) -> result.Result:
        sql.add_pidal(1)
        backend = await self.backend_manager.get_backend(node.node, self.xid)
        return await backend.query(before_sql + str(sql.raw))

    @staticmethod
    def _modify_table(node: DBTableStrategyBackend, sql: DML):
        sql.modify_table(node.prefix + str(node.number))

    async def _update_undo_log(self, lock_keys: Dict[str, Any],
                               sql: Update) -> Optional[result.Error]:
        table = self.db.get_table(str(sql.table))
        table_logs = self._reundo_log.get(table.get_name(), None)

        if table_logs is None:
            table_logs = {}
            self._reundo_log[table.get_name()] = table_logs
        log = table_logs.get(str(lock_keys), None)

        if log is None:
            # 获取 undo log 并获取本地锁。
            sl = sqlparse.parse("select * from {} ".format(
                str(sql.table)))[0]
            sl.insert_after(len(sl.tokens), sql.get_where())
            sl.insert_after(len(sl.tokens), sqlparse.parse(" for update")[0])
            old_raw = await table.execute_dml(Select(sl), self.xid)
            if isinstance(old_raw, result.Error):
                return old_raw
            elif not isinstance(old_raw, result.ResultSet):
                raise Exception("can`t get undo log.")

            self._record_undo_data(table.get_name(), lock_keys, old_raw)
            log = self._reundo_log[table.get_name()][str(lock_keys)]
        log.set_redo(sql.new_value, A2PCOperation.UPDATE)

    def reundo_log_generator(self, table: str, lock_keys: Dict[str, Any])\
            -> Optional[str]:
        table_logs = self._reundo_log.get(table, None)

        if table_logs is None:
            return None
        log = table_logs.get(str(lock_keys), None)

        if log is None:
            return None

        return log.to_sql(A2PCStatus.ACTIVE)

    async def execute_insert(self, sql: Insert) -> result.Result:
        table = self.db.get_table(str(sql.table))
        lock_keys = {}
        node = table.get_node(sql)
        for i in table.get_lock_columns():
            if i not in sql.new_value.keys():
                raise Exception("lock data need [{}] column.".format(i))
            lock_keys[i] = sql.new_value[i]

        c = []
        for i in node:
            self._modify_table(i, sql)
            error = await self._insert_undo_log(lock_keys, sql)
            if error:
                return error  # type: ignore
            reundo_sql = self.reundo_log_generator(str(sql.table), lock_keys)
            if not reundo_sql:
                raise Exception("can not get redo undo log.")
            before_sql = "begin;" + reundo_sql
            c.append(self._execute_dml(i, before_sql, sql))

        # 先计算好数据在上锁。减少锁定时间。
        lock_c = self.a2pc_client.acquire_lock(self.xid, node[0].node,
                                               str(sql.table), lock_keys,
                                               str(sql.raw))
        c.append(lock_c)
        r = await asyncio.gather(*c)
        return await self._execute_ending(r, node, sql)  # type: ignore

    async def _execute_ending(self,
                              r: List[Union[result.Result, A2PCResponse]],
                              node: List[DBTableStrategyBackend], sql: DMLW):
        lock = r[-1]
        r = r[:-1]
        ending_sql = "COMMIT"
        return_v = None
        if not isinstance(lock, A2PCResponse):
            ending_sql = "ROLLBACK"
            return_v = result.Error(1000, "{} acquire lock fail: {}".format(
                             str(sql.raw), str(lock)))
        elif lock.status != 0:
            logger.warning("{} acquire lock fail: {}".format(
                             str(sql.raw), lock.msg))
            ending_sql = "ROLLBACK"
            return_v = result.Error(1000, "{} acquire lock fail: {}".format(
                             str(sql.raw), lock.msg))
        if any([isinstance(i, result.Error) for i in r]):
            errors = [i for i in r if isinstance(i, result.Error)]
            for i in errors:
                logger.warning("{} error with code[{}],msg: {}".format(
                                 str(sql.raw), i.error_code, i.message))
            ending_sql = "ROLLBACK"
            return_v = errors[0]
        c = []
        for i in node:
            c.append(self._execute_ending_sql(ending_sql, i))
        # 如果在 rollback 的时候异常，抛错给 app，即使数据库没有 rollback，
        # 也会在超时机制下回滚。 backend 遇到异常会关闭 session 触发 mysql
        # 的超时
        # 如果在 commit 的时候出现异常，返回错误给客户端，客户端可以重试，
        # 成功就走正常流程，或者选择回滚，回滚就走事务回滚流程
        await asyncio.gather(*c)
        if ending_sql == "ROLLBACK":
            return return_v  # type: ignore
        return result.OK(1, 0, 2, 0, "", False)

    async def _insert_undo_log(self, lock_keys: Dict[str, Any],
                               sql: Insert) -> Optional[result.Error]:
        table_name = str(sql.table)
        table_logs = self._reundo_log.get(table_name, None)

        if table_logs is None:
            table_logs = {}
            self._reundo_log[table_name] = table_logs
        log = table_logs.get(str(lock_keys), None)

        if log is None:
            self._record_undo_data(table_name, lock_keys, None)
            log = self._reundo_log[table_name][str(lock_keys)]
        log.set_redo(sql.new_value, A2PCOperation.INSERT)

    async def execute_delete(self, sql: Delete) -> result.Result:
        table = self.db.get_table(str(sql.table))
        if not sql.raw_where:
            raise Exception("delete must contain [where].")

        lock_keys = {}
        node = table.get_node(sql)
        for i in table.get_lock_columns():
            if i not in sql.raw_where.keys():
                raise Exception("lock data need [{}] column.".format(i))
            lock_keys[i] = sql.raw_where[i]

        error = await self._delete_undo_log(lock_keys, sql)
        if not error:
            return error  # type: ignore
        reundo_sql = self.reundo_log_generator(table.get_name(), lock_keys)
        if not reundo_sql:
            raise Exception("can not get redo undo log.")
        before_sql = "begin;" + reundo_sql
        c = []
        for i in node:
            c.append(self._execute_update(table, i, before_sql, sql))

        # 先计算好数据在上锁。减少锁定时间。
        lock_c = self.a2pc_client.acquire_lock(self.xid, node[0].node,
                                               str(sql.table), lock_keys,
                                               str(sql.raw))
        c.append(lock_c)
        r = await asyncio.gather(*c)
        lock = r[-1]
        r = r[:-1]
        ending_sql = "COMMIT"
        return_v = None
        if lock.status != 0:
            logger.warning("{} acquire lock fail: {}".format(
                             str(sql.raw), lock.msg))
            ending_sql = "ROLLBACK"
            return_v = result.Error(1000, "{} acquire lock fail: {}".format(
                             str(sql.raw), lock.msg))
        if any([isinstance(i, result.Error) for i in r]):
            errors = [i for i in r if isinstance(i, result.Error)]
            for i in errors:
                logger.warning("{} error with code[{}],msg: {}".format(
                                 str(sql.raw), i.error_code, i.message))
            ending_sql = "ROLLBACK"
            return_v = errors[0]
        c = []
        for i in node:
            c.append(self._execute_ending_sql(ending_sql, i))

        await asyncio.gather(*c)
        if ending_sql == "ROLLBACK":
            return return_v  # type: ignore
        return result.OK(1, 0, 2, 0, "", False)

    async def _delete_undo_log(self, lock_keys: Dict[str, Any],
                               sql: Delete) -> Optional[result.Error]:
        table = self.db.get_table(str(sql.table))
        table_logs = self._reundo_log.get(table.get_name(), None)

        if table_logs is None:
            table_logs = {}
            self._reundo_log[table.get_name()] = table_logs
        log = table_logs.get(str(lock_keys), None)

        if log is None:
            # 获取 undo log 并获取本地锁。
            sl = sqlparse.parse("select * from {} for update".format(
                str(sql.table)))[0]
            sl.insert_after(len(sl.tokens), sql.get_where())
            old_raw = await table.execute_dml(Select(sl), self.xid)
            if isinstance(old_raw, result.Error):
                return old_raw
            elif not isinstance(old_raw, result.ResultSet):
                raise Exception("can`t get undo log.")

            self._record_undo_data(table.get_name(), lock_keys, old_raw)
            log = self._reundo_log[table.get_name()][str(lock_keys)]
        log.set_redo(None, A2PCOperation.DELETE)

    async def execute_other(self, sql: SQL) -> result.Result:
        return await self.db.execute_other(sql)

    async def close(self):
        if self.status is not TransStatus.END:
            await self.rollback(None)
        self.backend_manager.free_trans(self.xid)
        logger.debug("xid: {} is closed".format(self.xid))
        for i in self.backend_manager.backends.keys():
            logger.debug(self.backend_manager.backends[i]._used)

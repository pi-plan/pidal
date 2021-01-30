import asyncio
import json
import time

from typing import Any, Dict, List

from pidal.node.connection import Connection
from pidal.dservice.transaction.a2pc.client.constant import A2PCStatus
from pidal.node.result import result
from pidal.meta.model import DBConfig, DBNode
from pidal.dservice.backend.backend_manager import BackendManager


select_roll_sql = "select * from transaction_info_{} where status = {} \
        order by update_time limit 100;"
lock_xid_sql = "begin;select * from transaction_info_{} where xid = {} \
        for update"
xid_finish_sql = "update transaction_info_{} set `status` = {} where xid = {};\
        commit"
find_all_lock_sql = "select * from lock_table_{} where xid = {}"
get_unredo_sql = "select * from reundo_log where xid = {xid} \
        and `table` = '{table}' and lock_key = '{lock_key}'"
lock_target_line = "begin;select * from {} where {} for update"
rollback_sql = "update {} set {} where {};commit;"
rollback_timeout = "BEGIN;UPDATE transaction_info_{} set `status` = {} \
        where update_time <= '{}' and status = 1;COMMIT"


class Rollbacker(object):

    def __init__(self, conf: DBConfig):
        self.conf = conf
        self.backend_manager = BackendManager.get_instance()
        self.rm_nodes: Dict[str, DBNode] = self.conf.nodes
        self.tm_nodes: List[DBNode] = self.conf.a2pc.backends
        self._add_tm_backend()
        self._add_rm_backend()

    def get_number(self, num: int) -> int:
        return num % len(self.tm_nodes)

    def _add_tm_backend(self):
        for i in self.tm_nodes:
            self.backend_manager.add_backend(i)

    def _add_rm_backend(self):
        for i in self.rm_nodes.values():
            self.backend_manager.add_backend(i)

    async def start(self):
        c = []
        for i, n in enumerate(self.tm_nodes):
            await self._start_tm_timeout(i, n.name)
            c.append(self._start_tm(i, n.name))
        await asyncio.gather(*c)

    async def _start_tm_timeout(self, number: int, node: str):
        tm = await self.backend_manager.get_backend(node)
        roll_time = time.strftime("%Y-%m-%d %H:%M:%S",
                                  time.localtime(time.time() - 600))
        cur = await tm.query(rollback_timeout.format(number,
                                                     A2PCStatus.ROLLBACKING,
                                                     roll_time))
        if not cur or isinstance(cur, result.Error):
            raise Exception(cur.error_code, cur.message)

    async def _start_tm(self, number: int, node: str):
        tm = await self.backend_manager.get_backend(node)
        cur = await tm.batch(select_roll_sql.format(number,
                                                    A2PCStatus.ROLLBACKING))
        self.backend_manager.release(node, tm)
        if not cur or isinstance(cur, result.Error):
            raise Exception(cur.error_code, cur.message)
        r = await cur.fetchall()
        if not r:
            return
        for i in r:
            await self._rollback_xid(i["xid"])

    async def _rollback_xid(self, xid: int):
        number = self.get_number(xid)
        tm = await self.backend_manager.get_backend(self.tm_nodes[number].name)
        xid_cur = await tm.batch(lock_xid_sql.format(number, xid))
        if isinstance(xid_cur, result.Error):
            await tm.rollback()
            self.backend_manager.release(self.tm_nodes[number].name, tm)
            return
        while await xid_cur.nextset():
            pass
        xidinfo = await xid_cur.fetchone()
        if xidinfo["status"] != A2PCStatus.ROLLBACKING.value:
            await tm.rollback()
            self.backend_manager.release(self.tm_nodes[number].name, tm)
            return
        res = await self._get_all_locks(xid)
        for line in res:
            await self._rollback_line(line)
        await tm.query(xid_finish_sql.format(number, A2PCStatus.ROLLBACKED,
                                             xid))
        self.backend_manager.release(self.tm_nodes[number].name, tm)

    async def _rollback_line(self, lock: Dict[str, Any]):
        rm = await self.backend_manager.get_backend(lock["node"])
        cur = await rm.batch(get_unredo_sql.format(**lock))
        if not cur or isinstance(cur, result.Error):
            self.backend_manager.release(lock["node"], rm)
            raise Exception(cur.error_code, cur.message)
        r = await cur.fetchone()
        if not r:
            return
        reundo_log = json.loads(r["reundo_log"])
        lock_key_map = json.loads(r["lock_key"])
        if reundo_log["operation"] == "INSERT":
            await self._rollback_line_insert(rm, lock["node"], lock["table"],
                                             lock_key_map, reundo_log)
        elif reundo_log["operation"] == "UPDATE":
            await self._rollback_line_update(rm, lock["node"], lock["table"],
                                             lock_key_map, reundo_log)
        elif reundo_log["operation"] == "DELETE":
            await self._rollback_line_delete(rm, lock["node"], lock["table"],
                                             lock_key_map, reundo_log)
        else:
            raise Exception("unknown operation:[{}]".format(reundo_log))

    async def _rollback_line_update(self, rm: Connection,
                                    node: str, table: str,
                                    lock: Dict[str, Any],
                                    reundo_log: Dict[str, Any]):
        where = " and ".join(
                ["{} = {}".format(k, v) for k, v in lock.items()])
        line_c = await rm.batch(lock_target_line.format(table, where))
        if not line_c or isinstance(line_c, result.Error):
            self.backend_manager.release(node, rm)
            raise Exception(line_c.error_code, line_c.message)
        while await line_c.nextset():
            pass
        line = await line_c.fetchone()

        need_update = {}
        for k, v in reundo_log["undo"].items():
            if line[k] != v:
                need_update[k] = v
        if need_update:
            set_str = ",".join(
                    ["{} = '{}'".format(k, v) for k, v in need_update.items()])
            rollback = rollback_sql.format(table, set_str, where)
            rr = await rm.query(rollback)
            if isinstance(rr, result.Error):
                self.backend_manager.release(node, rm)
                raise Exception(rr.error_code, rr.message)

        self.backend_manager.release(node, rm)

    async def _rollback_line_delete(self, rm: Connection,
                                    node: str, table: str,
                                    lock: Dict[str, Any],
                                    reundo_log: Dict[str, Any]):
        where = " and ".join(
                ["{} = {}".format(k, v) for k, v in lock.items()])
        line_c = await rm.batch(lock_target_line.format(table, where))
        if not line_c or isinstance(line_c, result.Error):
            self.backend_manager.release(node, rm)
            raise Exception(line_c.error_code, line_c.message)
        while await line_c.nextset():
            pass
        line = await line_c.fetchone()
        if line:
            for k, v in line.items():
                rv = reundo_log["undo"].get(k)
                if rv != v:
                    # 理论上不会出现这个场景，这里作为监控
                    raise Exception("data has changed, before rollback.")
        columns = ",".join(list(reundo_log["undo"].keys()))
        values = ",".join(
                ["'{}'".format(i) for i in reundo_log["undo"].values()])
        rr = await rm.query("INSERT INTO {} ({}) VALUES ({});COMMIT;".format(
            table, columns, values))
        if isinstance(rr, result.Error):
            self.backend_manager.release(node, rm)
            raise Exception(rr.error_code, rr.message)
        self.backend_manager.release(node, rm)

    async def _rollback_line_insert(self, rm: Connection,
                                    node: str, table: str,
                                    lock: Dict[str, Any],
                                    reundo_log: Dict[str, Any]):
        where = " and ".join(
                ["{} = {}".format(k, v) for k, v in lock.items()])
        line_c = await rm.batch(lock_target_line.format(table, where))
        if not line_c or isinstance(line_c, result.Error):
            self.backend_manager.release(node, rm)
            raise Exception(line_c.error_code, line_c.message)
        while await line_c.nextset():
            pass
        line = await line_c.fetchone()
        if not line:
            return
        for k, v in line.items():
            if not isinstance(v, str):
                v = str(v)
            if k in ("id", "pidal_c"):  # TODO 允许配置
                continue
            rv = reundo_log["redo"].get(k)
            if rv != v:
                # 理论上不会出现这个场景，这里作为监控
                raise Exception("data has changed, before rollback.")
        rr = await rm.query("DELETE FROM {} WHERE {};COMMIT;".format(
            table, where))
        if isinstance(rr, result.Error):
            self.backend_manager.release(node, rm)
            raise Exception(rr.error_code, rr.message)
        self.backend_manager.release(node, rm)

    async def _get_all_locks(self, xid: int) -> List[Dict]:
        c = []
        for i, n in enumerate(self.tm_nodes):
            b = await self.backend_manager.get_backend(n.name)
            c.append(b.batch(find_all_lock_sql.format(i, xid)))
            self.backend_manager.release(n.name, b)

        r = await asyncio.gather(*c)
        res = []
        for i in r:
            if isinstance(i, result.Error):
                raise Exception(i.error_code, i.message)

            res += await i.fetchall()
        return res

    async def rollback_xid(self, xid: int):
        await self._rollback_xid(xid)

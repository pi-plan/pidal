import asyncio
import json

from typing import Any, Dict, List

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
            c.append(self._start_tm(i, n.name))
        await asyncio.gather(*c)

    async def _start_tm(self, number: int, node: str):
        tm = await self.backend_manager.get_backend(node)
        cur = await tm.batch(select_roll_sql.format(number,
                                                    A2PCStatus.ROLLBACKING))
        self.backend_manager.release(node, tm)
        if not cur or isinstance(cur, result.Error):
            raise Exception(cur.error_code, cur.message)
        r = await cur.fetchall()
        print("rollback: {}{}".format(r, tm))
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
        reundo_log = json.loads(r["reundo_log"])
        lock_key_map = json.loads(r["lock_key"])
        where = " and ".join(
                ["{} = {}".format(k, v) for k, v in lock_key_map.items()])
        line_c = await rm.batch(lock_target_line.format(lock["table"], where))
        if not line_c or isinstance(line_c, result.Error):
            self.backend_manager.release(lock["node"], rm)
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
            rollback = rollback_sql.format(lock["table"], set_str, where)
            rr = await rm.query(rollback)
            if isinstance(rr, result.Error):
                self.backend_manager.release(lock["node"], rm)
                raise Exception(rr.error_code, rr.message)

        self.backend_manager.release(lock["node"], rm)

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

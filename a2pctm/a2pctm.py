import asyncio
import json
import hashlib

from typing import Any, Callable, Dict, Optional

from pidal.node.result import result
from pidal.dservice.transaction.a2pc.client.constant import A2PCStatus
from pidal.dservice.transaction.a2pc.client.protocol import Protocol
from pidal.dservice.backend.backend_manager import BackendManager
from pidal.lib.snowflake import generator as snowflake
from pidal.meta.model import A2PC


class A2PCTM(object):
    def __init__(self, conf: A2PC, rollback: Callable):
        self.conf = conf
        self.backend_manager = BackendManager.get_instance()
        self._add_backend()
        self.nodes = self.conf.backends
        self.mod = len(self.nodes)
        self.rollback_func = rollback

    def _add_backend(self):
        for i in self.conf.backends:
            self.backend_manager.add_backend(i)

    def get_table_number(self, num: int) -> int:
        return num % self.mod

    async def begin(self, p: Protocol) -> Dict[str, Any]:
        if not p.xid:
            p.xid = next(snowflake)

        sql = "BEGIN;INSERT INTO transaction_info_{} (`xid`, `status`, \
                `client_id`) VALUES ({}, {}, '{}');COMMIT;"
        number = self.get_table_number(p.xid)
        node = await self.backend_manager.get_backend(self.nodes[number].name)
        r = await node.query(sql.format(
                           number, p.xid, A2PCStatus.ACTIVE, ''))
        if isinstance(r, result.Error):
            return {"status": r.error_code, "message": r.message, "xid": p.xid}
        else:
            return {"status": 0, "message": "", "xid": p.xid}

    async def commit(self, p: Protocol) -> Dict[str, Any]:
        sql = "BEGIN;update transaction_info_{number} set `status` = {status} \
                where xid = {xid} and status in (1, 2);COMMIT;\
                select * from transaction_info_{number} where xid = {xid}"
        number = self.get_table_number(p.xid)
        node = await self.backend_manager.get_backend(self.nodes[number].name)
        params = {
                "number": number,
                "xid": p.xid,
                "status": A2PCStatus.COMMIT
                }
        cur = await node.batch(sql.format(**params))
        if isinstance(cur, result.Error):
            return {"status": cur.error_code,
                    "xid": p.xid, "message": cur.message}
        r = await cur.fetchone()
        while await cur.nextset():
            r = await cur.fetchone()
        if not r:
            return {"status": 1003, "xid": p.xid,
                    "message": "transaction not found"}
        if r["status"] == A2PCStatus.COMMIT.value:
            return {"status": 0, "xid": p.xid, "message": ""}
        else:
            return {"status": 1003, "xid": p.xid,
                    "message": "transaction is {}.".format(
                        A2PCStatus(r.rows[0][1]).name)}

    async def rollback(self, p: Protocol) -> Dict[str, Any]:
        sql = "BEGIN;update transaction_info_{number} set `status` = {status} \
                where xid = {xid} and status in (1, 3);COMMIT;\
                select * from transaction_info_{number} where xid = {xid}"
        number = self.get_table_number(p.xid)
        node = await self.backend_manager.get_backend(self.nodes[number].name)
        params = {
                "number": number,
                "xid": p.xid,
                "status": A2PCStatus.ROLLBACKING
                }
        cur = await node.batch(sql.format(**params))
        if isinstance(cur, result.Error):
            return {"status": cur.error_code,
                    "xid": p.xid, "message": cur.message}
        r = await cur.fetchone()
        while await cur.nextset():
            r = await cur.fetchone()
        if not r:
            return {"status": 1003, "xid": p.xid,
                    "message": "transaction not found"}
        if r["status"] == A2PCStatus.ROLLBACKING.value:
            asyncio.create_task(self.rollback_func(p.xid))
            return {"status": 0, "xid": p.xid, "message": ""}
        elif r["status"] == A2PCStatus.ROLLBACKED.value:
            return {"status": 0, "xid": p.xid, "message": ""}
        else:
            return {"status": 1003, "xid": p.xid,
                    "message": "transaction is {}.".format(
                        A2PCStatus(r["status"]).name)}

    async def acquire_lock(self, p: Protocol) -> Optional[Dict[str, Any]]:
        lock_key = json.dumps(p.lock_key)
        int_lock = int(hashlib.md5(lock_key.encode()).hexdigest()[26:], 16)
        number = self.get_table_number(int_lock)
        before_sql = """
BEGIN;
INSERT IGNORE INTO lock_table_{number} (lock_key, xid, node, `table`) \
        VALUES ('{lock_key}', {xid}, '{node}', '{table}');
SELECT * FROM lock_table_{number} WHERE lock_key = '{lock_key}' \
        and node = '{node}' and `table` = '{table}' for update;
"""
        update_sql = """
UPDATE lock_table_{number} SET xid = {xid} WHERE lock_key = '{lock_key}' \
        and node = '{node}' and `table` = '{table}';
"""
        node = await self.backend_manager.get_backend(self.nodes[number].name)
        params = {
                "lock_key": lock_key,
                "xid": p.xid,
                "node": p.node,
                "table": p.table,
                "number": number
                }
        cur = await node.batch(before_sql.format(**params))
        if isinstance(cur, result.Error):
            self.backend_manager.release(self.nodes[number].name, node)
            return {"status": cur.error_code, "xid": p.xid,
                    "message": cur.message}
        try:
            # TODO 优化错误处理模式。
            while await cur.nextset():
                r = await cur.fetchall()
            if len(r) != 1:  # type: ignore
                await node.query("ROLLBACK")
                self.backend_manager.release(self.nodes[number].name, node)
                return {"status": 100001, "xid": p.xid,
                        "message": "multi lines."}
            r = r[0]  # type: ignore
            if r["xid"] == p.xid:
                await node.query("COMMIT")
                self.backend_manager.release(self.nodes[number].name, node)
                return {"status": 0, "xid": p.xid, "message": ""}
            old_status = await self._get_xid_status(r["xid"])
            if old_status in [A2PCStatus.COMMIT, A2PCStatus.ROLLBACKED]:
                cur = await node.batch((update_sql + "COMMIT").format(
                    **params))
                if isinstance(cur, result.Error):
                    self.backend_manager.release(self.nodes[number].name, node)
                    return {"status": cur.error_code, "xid": p.xid,
                            "message": cur.message}
                while await cur.nextset():
                    await cur.fetchall()
                r = node.read_result(cur._result)
                self.backend_manager.release(self.nodes[number].name, node)
                if isinstance(r, result.Error):
                    return {"status": r.error_code, "xid": p.xid,
                            "message": r.message}
                else:
                    return {"status": 0, "xid": p.xid, "message": ""}
            else:
                # 无法获取全局锁，重试
                await node.query("ROLLBACK")
                self.backend_manager.release(self.nodes[number].name, node)
                return None
        except Exception as e:
            await node.query("ROLLBACK")
            self.backend_manager.release(self.nodes[number].name, node)
            raise e

    async def _get_xid_status(self, xid: int) -> A2PCStatus:
        number = self.get_table_number(xid)
        node = await self.backend_manager.get_backend(self.nodes[number].name)
        sql = "SELECT * FROM transaction_info_{} WHERE xid = {}"
        cur = await node.batch(sql.format(number, xid))
        r = await cur.fetchone()  # type: ignore
        self.backend_manager.release(self.nodes[number].name, node)
        if not r:
            return A2PCStatus.ACTIVE
        else:
            return A2PCStatus(r["status"])

import json
import hashlib

from typing import Any, Dict
from pidal.node.result import result
from pidal.dservice.transaction.a2pc.client.constant import A2PCStatus
from pidal.dservice.transaction.a2pc.client.protocol import Protocol
from pidal.dservice.backend.backend_manager import BackendManager
from pidal.lib.snowflake import generator as snowflake
from pidal.meta.model import A2PC


class A2PCTM(object):
    def __init__(self, conf: A2PC):
        self.conf = conf
        self.backend_manager = BackendManager.get_instance()
        self._add_backend()
        self.nodes = self.conf.backends
        self.mod = len(self.nodes)

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
        sql = "BEGIN;INSERT INTO transaction_info_{} (`xid`, `status`, \
                `client_id`) VALUES ({}, {}, '{}') \
                ON DUPLICATE KEY UPDATE status = VALUES(`status`), \
                client_id = VALUES(`client_id`);COMMIT;"
        number = self.get_table_number(p.xid)
        node = await self.backend_manager.get_backend(self.nodes[number].name)
        r = await node.query(sql.format(number, p.xid, A2PCStatus.COMMIT, ''))
        if isinstance(r, result.Error):
            return {"status": r.error_code, "xid": p.xid, "message": r.message}
        else:
            return {"status": 0, "xid": p.xid, "message": ""}

    async def rollback(self, p: Protocol) -> Dict[str, Any]:
        sql = "BEGIN;INSERT INTO transaction_info_{} (`xid`, `status`, \
                `client_id`) VALUES ({}, {}, '{}') \
                ON DUPLICATE KEY UPDATE status = VALUES(`status`), \
                client_id = VALUES(`client_id`);COMMIT;"
        number = self.get_table_number(p.xid)
        node = await self.backend_manager.get_backend(self.nodes[number].name)
        r = await node.query(sql.format(number, p.xid, A2PCStatus.ROLLBACKING,
                                        ''))
        if isinstance(r, result.Error):
            return {"status": r.error_code, "xid": p.xid, "message": r.message}
        else:
            return {"status": 0, "xid": p.xid, "message": ""}

    async def acquire_lock(self, p: Protocol) -> Dict[str, Any]:
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
            return {"status": cur.error_code, "xid": p.xid,
                    "message": cur.message}
        try:
            # TODO 优化错误处理模式。
            while await cur.nextset():
                r = await cur.fetchall()
            if len(r) != 1:
                await node.query("ROLLBACK")
                return {"status": 100001, "xid": p.xid,
                        "message": "multi lines."}
            r = r[0]
            if r["xid"] == p.xid:
                await node.query("COMMIT")
                return {"status": 0, "xid": p.xid, "message": ""}
            old_status = await self._get_xid_status(r["xid"])
            if old_status in [A2PCStatus.COMMIT, A2PCStatus.ROLLBACKED]:
                cur = await node.batch((update_sql + "COMMIT").format(**params))
                if isinstance(cur, result.Error):
                    return {"status": cur.error_code, "xid": p.xid,
                            "message": cur.message}
                while await cur.nextset():
                    await cur.fetchall()
                r = node.read_result(cur._result)
                if isinstance(r, result.Error):
                    return {"status": r.error_code, "xid": p.xid,
                            "message": r.message}
                else:
                    return {"status": 0, "xid": p.xid, "message": ""}
            else:
                # 无法获取全局锁，重试
                await node.query("ROLLBACK")
                return None
        except Exception as e:
            await node.query("ROLLBACK")
            raise e

    async def _get_xid_status(self, xid: int) -> A2PCStatus:
        number = self.get_table_number(xid)
        node = await self.backend_manager.get_backend(self.nodes[number].name)
        sql = "SELECT * FROM transaction_info_{} WHERE xid = {}"
        cur = await node.batch(sql.format(number, xid))
        r = await cur.fetchone()  # type: ignore
        if not r:
            return A2PCStatus.ACTIVE
        else:
            return A2PCStatus(r["status"])

import json
from typing import Any, Dict, List
from pidal.node.result import result
from pidal.dservice.transaction.a2pc.client.constant import A2PCStatus
from pidal.dservice.transaction.a2pc.client.protocol import Protocol
from pidal.dservice.backend.backend_manager import BackendManager
from pidal.meta.model import A2PC


class A2PCTM(object):
    def __init__(self, conf: A2PC):
        self.conf = conf
        self.backend_manager = BackendManager.get_instance()
        self._add_backend()
        self.defalut_node = self.conf.backends[0]

    def _add_backend(self):
        for i in self.conf.backends:
            self.backend_manager.add_backend(i)

    async def commit(self, p: Protocol) -> Dict[str, Any]:
        node = await self.backend_manager.get_backend(self.defalut_node.name)
        r = await node.query(
                "update lock_table set status = {} where xid = {}".format(
                       A2PCStatus.COMMIT, p.xid))
        if isinstance(r, result.Error):
            return {"status": r.error_code, "message": r.message}
        else:
            return {"status": 0, "message": ""}

    async def rollback(self, p: Protocol) -> Dict[str, Any]:
        node = await self.backend_manager.get_backend(self.defalut_node.name)
        r = await node.query(
                "update lock_table set status = {} where xid = {}".format(
                       A2PCStatus.ROLLBACKING, p.xid))
        if isinstance(r, result.Error):
            return {"status": r.error_code, "message": r.message}
        else:
            return {"status": 0, "message": ""}

    async def acquire_lock(self, p: Protocol) -> Dict[str, Any]:
        before_sql = """
BEGIN;
INSERT IGNORE INTO lock_table (lock_key, xid, node, `table`, `status`) VALUES ('{lock_key}', {xid}, '{node}', '{table}', status);
SELECT * FROM lock_table WHERE lock_key = '{lock_key}' and node = '{node}' and table = '{table}' for update;
"""
        update_sql = """
UPDATE lock_table SET `status` = {status}, xid = {xid} WHERE lock_key = '{lock_key}' and node = '{node}' and table = '{table}';
"""
        node = await self.backend_manager.get_backend(self.defalut_node.name)
        params = {
                "lock_key": json.dumps(p.lock_key),
                "xid": p.xid,
                "node": p.node,
                "table": p.table,
                "status": A2PCStatus.ACTIVE
                }
        cur = await node.batch(before_sql.format(**params))
        try:
            while cur.nextset():
                pass
            r = cur.fetchall()
            if len(r) != 1:
                await node.query("ROLLBACK")
                return {"status": 100001, "message": "multi lines."}
            r = r[0]
            if r["xid"] == p.xid:
                await node.query("COMMIT")
                return {"status": 0, "message": ""}
            if A2PCStatus(res["status"]) in [A2PCStatus.COMMIT,
                                             A2PCStatus.ROLLBACKED]:
                cur = node.batch((update_sql + "COMMIT").format(params))
                while cur.nextset():
                    pass
                r = node.read_result(cur._result)
                if isinstance(r, result.Error):
                    return {"status": r.error_code, "message": r.message}
                else:
                    return {"status": 0, "message": ""}
            else:
                # 无法获取全局锁，重试
                await node.query("ROLLBACK")
                return None
        except Exception as e:
            await node.query("ROLLBACK")
            raise e

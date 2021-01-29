import asyncio
from pidal.dservice.transaction.a2pc.client.constant import A2PCStatus
from typing import Dict, List

from pidal.node.result import result
from pidal.meta.model import DBConfig, DBNode
from pidal.dservice.backend.backend_manager import BackendManager

select_to_clean_sql = "select * from transaction_info_{} where status in ({}) \
order by update_time limit 100;"
lock_xid_sql = "begin;select * from transaction_info_{} where xid = {} \
for update"
find_all_lock_node_sql = "select node from lock_table_{} where xid = {} \
group by `node`"
clean_reundolog = "begin;delete from reundo_log where xid = {};commit;"
clean_all_lock_node_sql = "begin;delete from lock_table_{} where xid = {};\
COMMIT;"
clean_xid_sql = "delete from transaction_info_{} where xid = {};COMMIT"


class Cleaner(object):

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
        clean_status = [str(A2PCStatus.ROLLBACKED.value),
                        str(A2PCStatus.COMMIT.value)]
        cur = await tm.batch(select_to_clean_sql.format(
                                            number, ",".join(clean_status)))
        self.backend_manager.release(node, tm)
        if not cur or isinstance(cur, result.Error):
            raise Exception(cur.error_code, cur.message)
        r = await cur.fetchall()
        print("cleaner: {}{}".format(r, tm))
        if not r:
            return
        for i in r:
            await self._clean_xid(i["xid"])

    async def _clean_xid(self, xid: int):
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
        if not xidinfo or xidinfo["status"] not in (
                A2PCStatus.ROLLBACKED.value,
                A2PCStatus.COMMIT.value):
            await tm.rollback()
            self.backend_manager.release(self.tm_nodes[number].name, tm)
            return
        res = await self._get_xid_locks(xid)
        for line in res:
            await self._clean_node_xid(xid, line["node"])
            await self._clean_xid_locks(xid)
        await tm.query(clean_xid_sql.format(number, xid))
        self.backend_manager.release(self.tm_nodes[number].name, tm)

    async def _clean_node_xid(self, xid: int, node: str):
        rm = await self.backend_manager.get_backend(node)
        r = await rm.query(clean_reundolog.format(xid))
        if isinstance(r, result.Error):
            self.backend_manager.release(node, rm)
            raise Exception(r)
        self.backend_manager.release(node, rm)

    async def _get_xid_locks(self, xid: int) -> List[Dict]:
        c = []
        for i, n in enumerate(self.tm_nodes):
            b = await self.backend_manager.get_backend(n.name)
            c.append(b.batch(find_all_lock_node_sql.format(i, xid)))
            self.backend_manager.release(n.name, b)

        r = await asyncio.gather(*c)
        res = []
        for i in r:
            if isinstance(i, result.Error):
                raise Exception(i.error_code, i.message)

            res += await i.fetchall()
        return res

    async def _clean_xid_locks(self, xid: int):
        c = []
        for i, n in enumerate(self.tm_nodes):
            b = await self.backend_manager.get_backend(n.name)
            c.append(b.query(clean_all_lock_node_sql.format(i, xid)))
            self.backend_manager.release(n.name, b)

        r = await asyncio.gather(*c)
        for i in r:
            if isinstance(i, result.Error):
                raise Exception(i)

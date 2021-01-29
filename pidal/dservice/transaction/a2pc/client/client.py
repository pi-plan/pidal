import json
from pidal.meta.manager import MetaManager

from typing import Any, Dict, List, Tuple, Optional

from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPResponse

from pidal.logging import logger
from pidal.dservice.transaction.a2pc.client.constant import A2PCAction


class A2PCResponse(object):

    def __init__(self, status: int, msg: str, xid: int = 0):
        self.status = status
        self.msg = msg
        self.xid = xid


class A2PClient(object):
    """
    接口：
    acquire_lock: for update 申请锁。
    commit: 提交事务
    rollback: 回滚事务
    """

    _instance: Optional['A2PClient'] = None

    @classmethod
    def new(cls) -> 'A2PClient':
        meta = MetaManager.get_instance()
        db_conf = meta.get_db()
        assert db_conf
        cls._instance = cls(db_conf.a2pc.servers)
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'A2PClient':  # type: ignore
        if not cls._instance:
            return cls.new()
        else:
            return cls._instance

    def __init__(self, services: List[Tuple[str, int]]):
        self.header = {}
        self.services = services
        self.mod = len(self.services) - 1

    async def begin(self, xid: int = 0) -> A2PCResponse:
        return await self._ending(xid, A2PCAction.BEGIN)

    async def acquire_lock(self, xid: int, node: str, table: str,
                           lock_key: Dict[str, Any], sql: str) -> A2PCResponse:
        body = json.dumps({
            "action": A2PCAction.ACQUIRE_LOCK,
            "xid": xid,
            "node": node,
            "table": table,
            "lock_key": lock_key,
            "context": sql
            })
        n = (xid % self.mod) if self.mod > 0 else 0
        server = self.services[n]
        url = "http://{}:{}/transactions".format(server[0], server[1])
        req = HTTPRequest(url, "PUT", self.header, body)
        client = AsyncHTTPClient()
        try:
            logger.info("put: {}, param: {}".format(url, body))
            response = await client.fetch(req)
            logger.info("response: {}".format(response.body))
            return self._parse_result(response)
        except Exception as e:
            logger.warning("Error: %s" % e)
            raise e

    async def commit(self, xid: int) -> A2PCResponse:
        return await self._ending(xid, A2PCAction.COMMIT)

    async def rollback(self, xid: int) -> A2PCResponse:
        return await self._ending(xid, A2PCAction.ROLLBACK)

    async def _ending(self, xid: int, action: A2PCAction) -> A2PCResponse:
        body = json.dumps({"action": action, "xid": xid})
        n = (xid % self.mod) if self.mod > 0 else 0
        server = self.services[n]
        url = "http://{}:{}/transactions".format(server[0], server[1])
        req = HTTPRequest(url, "PUT", self.header, body)
        client = AsyncHTTPClient()
        try:
            logger.info("put: {}, param: {}".format(url, body))
            response = await client.fetch(req)
            logger.info("response: {}".format(response.body))
            return self._parse_result(response)
        except Exception as e:
            logger.warning("Error: %s" % e)
            raise e

    @staticmethod
    def _parse_result(resp: HTTPResponse) -> A2PCResponse:
        r = json.loads(resp.body)
        status = r.get("status", None)
        xid = r.get("xid", 0)
        if status is None:
            raise Exception("response is error : {}".format(resp.body))
        msg = r.get("msg", None)
        return A2PCResponse(status, msg, xid)

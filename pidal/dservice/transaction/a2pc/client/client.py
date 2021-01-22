import json
from pidal.meta.manager import MetaManager

from typing import Any, Dict, List, Tuple, Optional

from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPResponse

from pidal.logging import logger
from pidal.dservice.transaction.a2pc.client.constant import A2PCAction


class A2PCResponse(object):

    def __init__(self, status: int, msg: str):
        self.status = status
        self.msg = msg


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
        return cls(db_conf.a2pc.servers)

    @classmethod
    def get_instance(cls) -> 'A2PClient':  # type: ignore
        if not cls._instance:
            cls.new()
        else:
            return cls._instance

    def __init__(self, services: List[Tuple[str, int]]):
        self.header = {}
        self.services = services
        self.mod = len(self.services) - 1

    async def acquire_lock(self, xid: int, node: str, table: str,
                           columns: Dict[str, Any], sql: str) -> A2PCResponse:
        body = json.dumps({"columns": columns, "sql": sql})
        server = self.services[xid % self.mod]
        url = "http://{}:{}/transactions/{}/{}/{}".format(server[0], server[1],
                                                          xid, node, table)
        req = HTTPRequest(url, "PUT", self.header, body)
        client = AsyncHTTPClient()
        try:
            logger.info("put: {}, param: {}".format(url, body))
            response = await client.fetch(req)
            return self._parse_result(response)
        except Exception as e:
            logger.warning("Error: %s" % e)
            raise e

    async def commit(self, xid: int) -> A2PCResponse:
        return await self._ending(xid, A2PCAction.COMMIT)

    async def rollback(self, xid: int) -> A2PCResponse:
        return await self._ending(xid, A2PCAction.ROLLBACK)

    async def _ending(self, xid: int, action: A2PCAction) -> A2PCResponse:
        body = json.dumps({"action": action})
        server = self.services[xid % self.mod]
        url = "http://{}:{}/transactions/{}".format(server[0], server[1], xid)
        req = HTTPRequest(url, "PUT", self.header, body)
        client = AsyncHTTPClient()
        try:
            logger.info("put: {}, param: {}".format(url, body))
            response = await client.fetch(req)
            return self._parse_result(response)
        except Exception as e:
            logger.warning("Error: %s" % e)
            raise e

    @staticmethod
    def _parse_result(resp: HTTPResponse) -> A2PCResponse:
        r = json.loads(resp.body)
        status = r.get("status", None)
        if status is None:
            raise Exception("response is error : {}".format(resp.body))
        msg = r.get("msg", None)
        return A2PCResponse(status, msg)

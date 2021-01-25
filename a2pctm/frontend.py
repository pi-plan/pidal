import asyncio
from pidal.dservice.backend.backend_manager import BackendManager
import tornado.web
import tornado.locks

from pidal.meta.manager import MetaManager
from pidal.dservice.transaction.a2pc.client.constant import A2PCAction
from pidal.dservice.transaction.a2pc.client.protocol import Protocol

from a2pctm.config import Config
from a2pctm.a2pctm import A2PCTM


class Frontend(tornado.web.Application):

    @classmethod
    def new(cls) -> 'Frontend':
        f = cls()
        return f

    def __init__(self):
        self.a2pc: A2PCTM
        self.prev_a2pc: A2PCTM
        self.meta_manager = MetaManager.new()
        BackendManager.new()
        handlers = [
            (r"/", Home),
            (r"/transactions", Transactions),
        ]
        super().__init__(handlers)
        self._get_latest()

    def _get_latest(self):
        version = self.meta_manager.get_latest_version()
        a2pc = self._create_a2pc(version)
        self.a2pc = a2pc

    def _create_a2pc(self, version: int) -> A2PCTM:
        db = self.meta_manager.get_db(version)
        assert db
        if not db.a2pc:
            raise Exception("unknown a2pc config.")
        return A2PCTM(db.a2pc)

    async def start(self):
        proxy = Config.get_proxy_config()
        self.listen(proxy.port, proxy.host)
        shutdown_event = tornado.locks.Event()
        await shutdown_event.wait()


class BaseHandler(tornado.web.RequestHandler):
    pass


class Home(BaseHandler):

    async def get(self):
        self.write("ok")


class Transactions(BaseHandler):

    async def put(self):
        p = Protocol.new(self.request.body)
        if p.action is A2PCAction.COMMIT:
            r = await self.application.a2pc.commit(p)
        elif p.action is A2PCAction.ROLLBACK:
            r = await self.application.a2pc.rollback(p)
        elif p.action is A2PCAction.ACQUIRE_LOCK:
            return self._acquire_lock(p)
        else:
            r = {"status": 1001, "msg": "unknown action."}
        self.write(r)

    async def _acquire_lock(self, p: Protocol):
        r = await self.application.a2pc.acquire_lock(p)
        if r is not None:
            return r
        times = 0
        while r is None:
            # TODO 修改成同步原语
            asyncio.sleep(0.1)
            r = await self.application.a2pc.acquire_lock(p)
            times += 1
            if times > 10:
                break
        if r is None:
            return {"status": 1003, "message": "can not acquire lock"}
        else:
            return r

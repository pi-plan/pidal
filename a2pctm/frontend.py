import asyncio
import tornado.web
import tornado.locks

from pidal.meta.manager import MetaManager
from pidal.dservice.transaction.a2pc.client.constant import A2PCAction
from pidal.dservice.transaction.a2pc.client.protocol import Protocol
from pidal.dservice.backend.backend_manager import BackendManager

from a2pctm.cleaner import Cleaner
from a2pctm.rollback import Rollbacker
from a2pctm.config import Config
from a2pctm.a2pctm import A2PCTM


class Frontend(tornado.web.Application):

    @classmethod
    def new(cls) -> 'Frontend':
        f = cls()
        return f

    def __init__(self):
        self.version: int = 0
        self.a2pc: A2PCTM
        self.prev_a2pc: A2PCTM
        self.rollbacker: Rollbacker
        self.prev_rollbacker: Rollbacker
        self.cleaner: Cleaner
        self.prev_cleaner: Cleaner
        self.meta_manager = MetaManager.new(
                Config.get_instance())  # type: ignore
        BackendManager.new()
        handlers = [
            (r"/", Home),
            (r"/transactions", Transactions),
        ]
        super().__init__(handlers)
        self._get_latest()
        self.loop = asyncio.get_event_loop()
        #  self.loop.call_later(6, lambda: asyncio.create_task(
            #  self.start_clean()))
        self.loop.call_later(6, lambda: asyncio.create_task(
            self.start_rollback()))

    async def _rollback_xid(self, xid: int):
        await self.rollbacker.rollback_xid(xid)

    def _get_latest(self):
        version = self.meta_manager.get_latest_version()
        self._create_version(version)

    def _create_version(self, version: int):
        if self.version == version:
            return
        if getattr(self, "a2pc", None):
            self.prev_a2pc = self.a2pc
        if getattr(self, "cleaner", None):
            self.prev_cleaner = self.cleaner
        if getattr(self, "rollbacker", None):
            self.prev_rollbacker = self.rollbacker

        self.version = version
        db_confg = self.meta_manager.get_db(version)
        assert db_confg
        assert db_confg.a2pc
        self.a2pc = A2PCTM(db_confg.a2pc, self._rollback_xid)
        self.rollbacker = Rollbacker(db_confg)
        self.cleaner = Cleaner(db_confg)

    async def start_clean(self):
        await self.cleaner.start()
        self.loop.call_later(6, lambda: asyncio.create_task(
            self.start_clean()))

    async def start_rollback(self):
        await self.rollbacker.start()
        self.loop.call_later(6, lambda: asyncio.create_task(
            self.start_rollback()))

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
        if p.action is A2PCAction.BEGIN:
            r = await self.application.a2pc.begin(p)
        elif p.action is A2PCAction.COMMIT:
            r = await self.application.a2pc.commit(p)
        elif p.action is A2PCAction.ROLLBACK:
            r = await self.application.a2pc.rollback(p)
        elif p.action is A2PCAction.ACQUIRE_LOCK:
            r = await self._acquire_lock(p)
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
            await asyncio.sleep(0.1)
            r = await self.application.a2pc.acquire_lock(p)
            times += 1
            if times > 10:
                break
        if r is None:
            return {"status": 1003, "xid": p.xid,
                    "message": "can not acquire lock"}
        else:
            return r

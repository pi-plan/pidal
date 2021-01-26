from typing import Optional, Set, Tuple, cast

from tornado.ioloop import IOLoop
from tornado.iostream import IOStream

from pidal.logging import logger
from pidal.config import Config, ProxyConfig
from pidal.frontend.tcpserver import ProxyTCPServer
from pidal.frontend.session import Session
from pidal.meta.manager import MetaManager
from pidal.dservice.dservice import DService
from pidal.dservice.backend.backend_manager import BackendManager


class Frontend(ProxyTCPServer):
    def __init__(self):
        super().__init__()
        self.sessions: Set[Session] = set()
        self._conf: ProxyConfig = ProxyConfig.get_instance()
        self.meta_manager = MetaManager.new(Config.get_instance())

        # 当前最新的 Dserver 服务，在启动的时候必须要初始化
        self.dserver: DService
        # 上一个版本的服务, 在服务升级期间会存在
        self.prev_dserver: Optional[DService] = None

    async def handle_stream(self, stream: IOStream, address: Tuple[str, int]):
        logger.info("get connection from %s:%s", *address)
        sess = Session(stream, address, self, self.dserver)
        self.sessions.add(sess)
        sess.start()

    def on_close(self, sess: Session):
        self.sessions.remove(cast(Session, sess))

    def on_recv():  # type: ignore
        pass

    def on_send():  # type: ignore
        pass

    def bootstrap(self):
        BackendManager.new()
        latest_version = self.meta_manager.get_latest_version()
        if hasattr(self, "dserver") and self.meta_manager.version_gt(
                   latest_version, self.dserver.get_version()):
            raise Exception("no latest version meta.")
        self.dserver = self.create_d_server(latest_version)

    def create_d_server(self, version) -> DService:
        zones = self.meta_manager.get_zones(version)
        db = self.meta_manager.get_db(version, self.meta_manager.zone_id)
        assert db
        dserver = DService(self.meta_manager.zone_id, version, zones, db)
        return dserver

    def start(self):
        self.bootstrap()
        self.listen(port=self._conf.port, address=self._conf.host)
        logger.info("start listen %s:%s", self._conf.host, self._conf.port)
        IOLoop.current().start()

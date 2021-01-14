from typing import List
from tornado.ioloop import IOLoop

from pidal.logging import logger
from pidal.config import ProxyConfig
from pidal.frontend.tcpserver import ProxyTCPServer
from pidal.meta.manager import MetaManager
from pidal.dservice.dservice import DService


class Frontend(ProxyTCPServer):
    def __init__(self):
        super().__init__()
        self._conf: ProxyConfig = ProxyConfig.get_instance()
        self.d_services: List[DService] = []
        self.meta_manager = MetaManager.new()

    def create_d_server(self) -> DService:
        latest_version = self.meta_manager.get_latest_version()
        if self.meta_manager.version_gt(latest_version, )


    def start(self):
        self.listen(port=self._conf.port, address=self._conf.host)
        logger.info("start listen %s:%s", self._conf.host, self._conf.port)
        IOLoop.current().start()

from typing import Tuple, Set, cast

from tornado.iostream import IOStream
from tornado.tcpserver import TCPServer

from pidal.frontend.session import Session
from pidal.connection_delegate import ConnectionDelegate
from pidal.logging import logger


class ProxyTCPServer(TCPServer, ConnectionDelegate):
    def __init__(self):
        super().__init__()

        self.sessions: Set[Session] = set()

    async def handle_stream(self, stream: IOStream, address: Tuple[str, int]):
        logger.info("get connection from %s:%s", *address)
        sess = Session(stream, address, self)
        self.sessions.add(sess)
        sess.start()

    def on_close(self, sess: Session):
        self.sessions.remove(cast(Session, sess))

    def on_recv():  # type: ignore
        pass

    def on_send():  # type: ignore
        pass

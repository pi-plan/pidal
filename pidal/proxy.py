import ssl
from typing import Any, Optional, Tuple, Union, Dict, Set, cast

from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado.tcpserver import TCPServer

from pidal.config import ProxyConfig
from pidal.connection import Connection
from pidal.connection_delegate import ConnectionDelegate
from pidal.logging import logger


class ProxyTCPServer(TCPServer, ConnectionDelegate):
    def __init__(
        self,
        ssl_options: Optional[Union[Dict[str, Any], ssl.SSLContext]] = None,
        max_buffer_size: Optional[int] = None,
        read_chunk_size: Optional[int] = None,
    ) -> None:
        super().__init__(ssl_options, max_buffer_size, read_chunk_size)

        self.conns: Set[Connection] = set()

    async def handle_stream(self, stream: IOStream, address: Tuple[str, int]):
        logger.info("get connection from %s:%s", *address)
        conn = Connection(stream, address, self)
        self.conns.add(conn)
        conn.start()

    def on_close(self, server_conn: Connection):
        self.conns.remove(cast(Connection, server_conn))

    def on_recv():  # type: ignore
        pass

    def on_send():  # type: ignore
        pass


class ProxyServer(object):
    def __init__(self):
        self.conf: Optional[ProxyConfig] = None

    def init_config(self):
        self.conf = ProxyConfig.get_instance()

    def start(self):
        self.init_config()
        tcpserver = ProxyTCPServer()
        tcpserver.listen(port=self.conf.port, address=self.conf.host)
        logger.info("start listen %s:%s", self.conf.host, self.conf.port)
        IOLoop.current().start()

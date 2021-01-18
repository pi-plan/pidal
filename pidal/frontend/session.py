import asyncio
from pidal.node.result.command import Command
from pidal.node.result.result import Execute

from typing import Tuple

import tornado.iostream as torio

import pidal.protocol.mysql as mysql

from pidal.dservice.dservice import DService
from pidal.dservice.dsession import DSession
from pidal.constant.db import SessionStatus
from pidal.authentication.handshake import Handshake
from pidal.connection_delegate import ConnectionDelegate
from pidal.logging import logger
from pidal.stream import IOStream

MAX_PACKET_LEN = 2**24-1


class Session(object):

    def __init__(self, stream: torio.IOStream, address: Tuple[str, int],
                 delegate: ConnectionDelegate, dserver: DService):
        self.stream: IOStream = IOStream(stream)
        self.address: Tuple[str, int] = address
        self.delegate: ConnectionDelegate = delegate
        self.status: SessionStatus = SessionStatus.SERVING
        self.dserver = dserver
        self.dsession: DSession = dserver.create_session()

    def start(self):
        loop = asyncio.get_running_loop()
        if self.status is SessionStatus.INIT:
            loop.create_task(self.start_handshake())
        elif self.status is SessionStatus.HANDSHAKED:
            loop.create_task(self.start_serving())

    async def start_handshake(self):
        try:
            logger.info("client %s:%s start handshake.", *self.address)
            self.status = SessionStatus.HANDSHAKEING
            auth = Handshake(self.stream)
            handshake_result = await auth.start()
            if handshake_result:
                self.status = SessionStatus.HANDSHAKED
                self.start()
            else:
                self.close()
        except Exception as e:
            logger.warning("error with %s", str(e))
            self.close()

    async def start_serving(self):
        try:
            while True:
                packet = await mysql.PacketBytesReader.read_execute_packet(
                        self.stream)
                execute = Execute(packet.length,
                                  Command(packet.command),
                                  packet.args, packet.query)
                r = await self.dsession.execute(execute)
                print(r)
        except torio.StreamClosedError:
            logger.warning("client has close with.")
            self.close()
        except Exception as e:
            logger.warning("error with %s", str(e))

    def get_session_status(self) -> SessionStatus:
        return self.dsession.get_session_status()

    def close(self):
        self.status = SessionStatus.CLOSE
        self.delegate.on_close(self)  # type: ignore

import asyncio
import traceback

from typing import Tuple

import tornado.iostream as torio

import pidal.protocol.mysql as mysql

from pidal.node.result.command import Command
from pidal.node.result.result import Execute
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
        self.status: SessionStatus = SessionStatus.INIT
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
                await self.close()
        except Exception as e:
            logger.warning("error with %s", str(e))
            await self.close()

    async def start_serving(self):
        try:
            i = 0
            while True:
                packet = await mysql.PacketBytesReader.read_execute_packet(
                        self.stream)
                execute = Execute(packet.length,
                                  Command(packet.command),
                                  packet.args, packet.query)
                r = await self.dsession.execute(execute)
                i += 1
                num = int(i < 7)
                if i > 6:
                    num = 0
                logger.info(r[0])
                if r is not None:
                    await mysql.ResultWriter.write(r, self.stream, num)
        except torio.StreamClosedError:
            logger.warning("client has close with.")
            await self.close()
        except Exception as e:
            traceback.print_exc()
            logger.warning("error with %s", str(e))

    def get_session_status(self) -> SessionStatus:
        return self.dsession.get_session_status()

    async def close(self):
        self.status = SessionStatus.CLOSE
        await self.dsession.close()
        self.delegate.on_close(self)  # type: ignore

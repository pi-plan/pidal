import asyncio
import enum

from typing import Tuple

import tornado.iostream as torio

import pidal.protocol.mysql as mysql

from pidal.authentication.handshake import Handshake
from pidal.connection_delegate import ConnectionDelegate
from pidal.logging import logger
from pidal.stream import IOStream
from pidal.pool.pool import PoolManager

MAX_PACKET_LEN = 2**24-1


@enum.unique
class SessionStatus(enum.IntFlag):
    CLOSE = 1
    INIT = 2  # 初始化
    HANDSHAKEING = 4  # 握手认证中
    HANDSHAKED = 8  # 已完成握手认证
    SERVING = 16  # 服务中
    IN_TRANSACTION = 32  # 在事务中


class Session(object):

    def __init__(self, stream: torio.IOStream, address: Tuple[str, int],
                 delegate: ConnectionDelegate):
        self.stream: IOStream = IOStream(stream)
        self.address: Tuple[str, int] = address
        self.delegate: ConnectionDelegate = delegate
        self.status: SessionStatus = SessionStatus.INIT

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
                conn = await PoolManager.acquire()
                r = await conn.query(packet.query)
                print(r)
        except torio.StreamClosedError:
            logger.warning("client has close with.")
            self.close()
        except Exception as e:
            logger.warning("error with %s", str(e))

    async def start_listing(self):
        try:
            while True:
                packet_header = await self.client.read_bytes(4)
                bytes_to_read, packet_number = self._parse_header(
                        packet_header)
                res = await self.client.read_bytes(bytes_to_read)
                p = mysql.PacketBytesReader(res)
                if p.is_ok_packet():
                    await self.stream.write(packet_header + res)
                elif p.is_eof_packet():
                    await self.stream.write(packet_header + res)
                elif p.is_error_packet():
                    await self.stream.write(packet_header + res)
                else:
                    r = await mysql.ResultSet.decode(res, self.client)
                    await r.encode(0, 2, self.stream, packet_number)
        except torio.StreamClosedError:
            logger.warning("client has close with.")
            self.close()
        except Exception as e:
            logger.warning("error with %s", str(e))

    def close(self):
        self.status = SessionStatus.CLOSE
        self.delegate.on_close(self)  # type: ignore


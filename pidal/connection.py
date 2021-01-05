import asyncio
import enum

from typing import Tuple

import tornado.iostream as torio

import pidal.err as err
import pidal.protocol.mysql as mysql

from pidal.authentication.handshake import Handshake
from pidal.connection_delegate import ConnectionDelegate
from pidal.logging import logger
from pidal.stream import IOStream
from pidal.pool.pool import PoolManager

MAX_PACKET_LEN = 2**24-1


@enum.unique
class ConnectionStatus(enum.IntEnum):
    INIT = 1
    HANDSHAKEING = 2
    HANDSHAKED = 3
    SERVING = 4
    CLOSE = 99


class Connection(object):

    def __init__(self, stream: torio.IOStream, address: Tuple[str, int],
                 delegate: ConnectionDelegate):
        self.stream: IOStream = IOStream(stream)
        self.address: Tuple[str, int] = address
        self.delegate: ConnectionDelegate = delegate
        self.status: ConnectionStatus = ConnectionStatus.INIT

    def start(self):
        loop = asyncio.get_running_loop()
        if self.status is ConnectionStatus.INIT:
            loop.create_task(self.start_handshake())
        elif self.status is ConnectionStatus.HANDSHAKED:
            loop.create_task(self.start_serving())

    async def start_handshake(self):
        try:
            logger.info("client %s:%s start handshake.", *self.address)
            self.status = ConnectionStatus.HANDSHAKEING
            auth = Handshake(self.stream)
            handshake_result = await auth.start()
            if handshake_result:
                self.status = ConnectionStatus.HANDSHAKED
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
        self.status = ConnectionStatus.CLOSE
        self.delegate.on_close(self)  # type: ignore


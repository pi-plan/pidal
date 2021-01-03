import asyncio
import enum
import struct

from typing import Tuple

from tornado.iostream import IOStream, StreamClosedError
from tornado.tcpclient import TCPClient

import pidal.err as err
import pidal.protocol.mysql.packet as mysql

from pidal.connection_delegate import ConnectionDelegate
from pidal.logging import logger
from pidal.protocol.util import dump_packet, byte2int

MAX_PACKET_LEN = 2**24-1


@enum.unique
class ConnectionStatus(enum.IntEnum):
    INIT = 1
    HANDSHAKEING = 2
    HANDSHAKED = 3
    SERVING = 4
    CLOSE = 99


class Connection(object):

    def __init__(self, stream: IOStream, address: Tuple[str, int],
                 delegate: ConnectionDelegate):
        self.stream = stream
        self.address = address
        self.delegate = delegate
        self.status = ConnectionStatus.INIT

    def start(self):
        loop = asyncio.get_running_loop()
        if self.status is ConnectionStatus.INIT:
            loop.create_task(self.start_handshake())
        elif self.status is ConnectionStatus.HANDSHAKED:
            loop.create_task(self.start_serving())
            loop.create_task(self.start_listing())

    async def start_handshake(self):
        try:
            logger.info("client %s:%s start handshake.", *self.address)
            self.status = ConnectionStatus.HANDSHAKEING
            client = await TCPClient().connect("127.0.0.1", 3306)  # TODO
            packet_header = await client.read_bytes(4)
            bytes_to_read, packet_number = self._parse_header(packet_header)
            if packet_number != 0:
                logger.error("recv to xxx handshake packet_number is %s",
                             packet_number)

            recv_data = await client.read_bytes(bytes_to_read)
            await self.stream.write(packet_header + recv_data)

            packet_header = await self.stream.read_bytes(4)
            bytes_to_read, packet_number = self._parse_header(packet_header)
            recv_data = await self.stream.read_bytes(bytes_to_read)
            await client.write(packet_header + recv_data)

            packet_header = await client.read_bytes(4)
            bytes_to_read, packet_number = self._parse_header(packet_header)
            recv_data = await client.read_bytes(bytes_to_read)
            await self.stream.write(packet_header + recv_data)
            if recv_data[0] == 0x00:
                self.status = ConnectionStatus.HANDSHAKED
                self.client = client  # TODO remove when finished protocol dev
                self.start()
            elif recv_data[0] == 0xff:
                self.close()
        except StreamClosedError as e:
            logger.warning("client has close with.", str(e))
            self.close()
        except Exception as e:
            logger.warning("error with %s", str(e))
            self.close()

    async def start_serving(self):
        try:
            while True:
                packet = await self._read_command_packet()
                await self.client.write(packet)
        except StreamClosedError:
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
                    ok = mysql.OK.decode(res)
                elif p.is_eof_packet():
                    pass
                elif p.is_error_packet():
                    pass
                else:
                    r = await mysql.ResultSet.decode(res, self.client)
                    print(vars(r.fields[0]))
                await self.stream.write(packet_header + res)
        except StreamClosedError:
            logger.warning("client has close with.")
            self.close()
        except Exception as e:
            logger.warning("error with %s", str(e))


    async def _read_command_packet(self):
        packet_header = await self.stream.read_bytes(4)
        bytes_to_read, packet_number = self._parse_header(
                packet_header)
        if bytes_to_read > MAX_PACKET_LEN:
            raise err.ClientPackageExceedsLength(bytes_to_read)
        recv_data = await self.stream.read_bytes(bytes_to_read)
        return packet_header + recv_data

    async def _read_bytes(self):
        buff = bytes()
        _next_seq_id = 0
        while True:
            packet_header = await self.stream.read_bytes(4)
            bytes_to_read, packet_number = self._parse_header(
                                            packet_header)
            if packet_number != _next_seq_id:
                if packet_number == 0:
                    # MariaDB sends error packet with seqno==0 when shutdown
                    raise err.OperationalError(
                        "Lost connection to MySQL server during query")
                raise err.InternalError(
                    "Packet sequence number wrong - got %d expected %d"
                    % (packet_number, _next_seq_id))
            _next_seq_id = (_next_seq_id + 1) % 256

            recv_data = await self.stream.read_bytes(bytes_to_read)
            buff += recv_data
            # https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
            if bytes_to_read == 0xffffff:
                continue
            if bytes_to_read < MAX_PACKET_LEN:
                break

        return buff

    def close(self):
        self.status = ConnectionStatus.CLOSE
        self.delegate.on_close(self)  # type: ignore

    @staticmethod
    def _parse_header(packet_header: bytes) -> Tuple[int, int]:
        btrl, btrh, packet_number = struct.unpack('<HBB', packet_header)
        bytes_to_read = btrl + (btrh << 16)
        return (bytes_to_read, packet_number)

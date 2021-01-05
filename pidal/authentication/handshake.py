import struct

from typing import Tuple

from tornado.tcpclient import TCPClient

from pidal.logging import logger
from pidal.stream import IOStream
from pidal.protocol.mysql import PacketBytesReader


class Handshake(object):
    def __init__(self, stream: IOStream):
        self.stream = stream

    async def start(self) -> bool:
        # TODO PiDAL need authentication feature.
        client = await TCPClient().connect(
                "127.0.0.1", 3306, timeout=60)  # TODO
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
        p_reader = PacketBytesReader(recv_data)
        if p_reader.is_ok_packet():
            return True
        return False

    @staticmethod
    def _parse_header(packet_header: bytes) -> Tuple[int, int]:
        btrl, btrh, packet_number = struct.unpack('<HBB', packet_header)
        bytes_to_read = btrl + (btrh << 16)
        return (bytes_to_read, packet_number)

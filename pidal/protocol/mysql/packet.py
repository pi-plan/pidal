import struct

from typing import Any, Awaitable, Optional, Tuple

import pidal.err as err

from pidal.logging import logger
from pidal.protocol.mysql import Command, ServerStatus
from pidal.protocol.util import byte2int, dump_packet
from pidal.protocol.mysql.stream import Stream


NULL_COLUMN = 251
UNSIGNED_CHAR_COLUMN = 251
UNSIGNED_SHORT_COLUMN = 252
UNSIGNED_INT24_COLUMN = 253
UNSIGNED_INT64_COLUMN = 254


class PacketHeader(object):
    payload_length: int
    packet_number: int

    @classmethod
    def decode(cls, packet_header: bytes) -> 'PacketHeader':
        header = cls()
        btrl, btrh, packet_number = struct.unpack('<HBB', packet_header)
        header.payload_length = btrl + (btrh << 16)
        header.packet_number = packet_number
        return header


class PacketBytesReader(object):

    def __init__(self, raw: bytes):
        self._data: bytes = raw
        self._position: int = 0

    def advance(self, length):
        new_position = self._position + length
        if new_position < 0 or new_position > len(self._data):
            error_msg = "Invalid advance amount ({}) for cursor. \
                    Position={}".format(length, new_position)
            logger.error(error_msg)
            raise Exception(error_msg)
        self._position = new_position

    def rewind(self, position=0):
        if position < 0 or position > len(self._data):
            error_msg = "Invalid position to rewind cursor to: {}.".format(
                    position)
            logger.error(error_msg)
            raise Exception(error_msg)
        self._position = position

    def read_all(self):
        result = self._data[self._position:]
        self._position = -1  # ensure no subsequent read()
        return result

    def read(self, size) -> bytes:
        result = self._data[self._position:(self._position+size)]
        if len(result) != size:
            error_msg = "Result length not requested length. \
                        Expected={}.  Actual={}.\
                        Position: {}.  Data Length: {}".format(
                        size, len(result), self._position, len(self._data))
            logger.error(error_msg)
            raise AssertionError(error_msg)
        self._position += size
        return result

    def read_uint8(self) -> int:
        result = self._data[self._position]
        self._position += 1
        return result

    def read_uint16(self) -> int:
        result = struct.unpack_from('<H', self._data, self._position)[0]
        self._position += 2
        return result

    def read_uint24(self) -> int:
        low, high = struct.unpack_from('<HB', self._data, self._position)
        self._position += 3
        return low + (high << 16)

    def read_uint32(self) -> int:
        result = struct.unpack_from('<I', self._data, self._position)[0]
        self._position += 4
        return result

    def read_uint64(self) -> int:
        result = struct.unpack_from('<Q', self._data, self._position)[0]
        self._position += 8
        return result

    def read_length_encoded_integer(self) -> int:
        c = self.read_uint8()
        if c < UNSIGNED_CHAR_COLUMN:
            return c
        elif c == UNSIGNED_SHORT_COLUMN:
            return self.read_uint16()
        elif c == UNSIGNED_INT24_COLUMN:
            return self.read_uint24()
        elif c == UNSIGNED_INT64_COLUMN:
            return self.read_uint64()
        logger.error("unknown length integer %s.", c)
        raise Exception("unknown length integer {}.".format(c))

    def read_length_coded_string(self) -> Optional[bytes]:
        c = self.read_uint8()
        if c == NULL_COLUMN:
            return None
        length = 0
        if c < UNSIGNED_CHAR_COLUMN:
            length = c
        elif c == UNSIGNED_SHORT_COLUMN:
            length = self.read_uint16()
        elif c == UNSIGNED_INT24_COLUMN:
            length = self.read_uint24()
        elif c == UNSIGNED_INT64_COLUMN:
            length = self.read_uint64()
        return self.read(length)

    def read_struct(self, fmt) -> Tuple[Any, ...]:
        s = struct.Struct(fmt)
        result = s.unpack_from(self._data, self._position)
        self._position += s.size
        return result

    def is_ok_packet(self):
        # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
        return self._data[0:1] == b'\0' and len(self._data) >= 7

    def is_eof_packet(self):
        # http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet
        # Caution: \xFE may be LengthEncodedInteger.
        # If \xFE is LengthEncodedInteger header, 8bytes followed.
        return self._data[0:1] == b'\xfe' and len(self._data) < 9

    def is_auth_switch_request(self):
        # http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
        return self._data[0:1] == b'\xfe'

    def is_extra_auth_data(self):
        # https://dev.mysql.com/doc/internals/en/successful-authentication.html
        return self._data[0:1] == b'\x01'

    def is_resultset_packet(self):
        field_count = ord(self._data[0:1])
        return 1 <= field_count <= 250

    def is_error_packet(self):
        return self._data[0:1] == b'\xff'


class Execute(object):

    length: int
    command: Command
    args: bytes
    query: str

    @classmethod
    def decode(cls, raw: bytes) -> 'Execute':
        p = cls()
        try:
            p.command = Command(raw[4])
        except ValueError:
            logger.warning("unknown command %s, with packet: %s",
                           byte2int(raw[0]), dump_packet(raw))
            raise err.OperationalError("unknown command {}.".format(
                                        byte2int(raw[0])))
        finally:
            p.args = raw[5:]
            if p.command is Command.COM_QUERY:
                p.query = p.args.decode()
        return p


class OK(object):
    affected_rows: int
    insert_id: int
    server_status: int
    warning_count: int
    message: str
    has_next: int

    @classmethod
    def decode(cls, raw: bytes) -> 'OK':
        p = cls()
        p_reader = PacketBytesReader(raw)
        p_reader.rewind()
        p_reader.advance(1)

        p.affected_rows = p_reader.read_length_encoded_integer()
        p.insert_id = p_reader.read_length_encoded_integer()
        p.server_status, p.warning_count = p_reader.read_struct('<HH')
        p.message = p_reader.read_all().decode()
        p.has_next = p.server_status & ServerStatus.SERVER_MORE_RESULTS_EXISTS
        return p


class EOF(object):
    server_status: int
    warning_count: int
    has_next: int

    @classmethod
    def decode(cls, raw: bytes):
        p = cls()
        p_reader = PacketBytesReader(raw)
        p_reader.rewind()

        p.warning_count, p.server_status = p_reader.read_struct('<xhh')
        p.has_next = p.server_status & ServerStatus.SERVER_MORE_RESULTS_EXISTS
        return p


class Error(object):
    error_code: int
    sql_state: str
    message: str

    @classmethod
    def decode(cls, raw: bytes):
        p = cls()
        p_reader = PacketBytesReader(raw)
        p_reader.rewind()
        p.error_code, p.sql_state = p_reader.read_struct("<xH6s")
        p.message = p_reader.read_all().decode()
        return p


class ResultSet(object):
    field_count: int

    @classmethod
    async def decode(cls, raw: bytes,
                     stream: Stream) -> Awaitable['ResultSet']:
        p = cls()
        p_reader = PacketBytesReader(raw)
        p.field_count = p_reader.read_length_encoded_integer()
        for i in range(p.field_count):
            header_bytes = await stream.read_bytes(4)
            header = PacketHeader.decode(header_bytes)
            body = await stream.read_bytes(header.payload_length)
        return p


class ResultSetHeader(object):
    catalog: Optional[bytes]
    db: Optional[bytes]
    table_name: str
    org_table: str
    name: str
    org_name: str
    charsetnr: int
    length: int
    type_code: int
    flags: int
    scale: int

    @classmethod
    def decode(cls, raw: bytes) -> 'ResultSetHeader':
        p = cls()
        p_reader = PacketBytesReader(raw)
        p_reader.rewind()
        p.catalog = p_reader.read_length_coded_string()
        p.db = p_reader.read_length_coded_string()
        p.table_name = p_reader.read_length_coded_string().decode()
        p.org_table = p_reader.read_length_coded_string().decode()
        p.name = p_reader.read_length_coded_string().decode()
        p.org_name = p_reader.read_length_coded_string().decode()
        p.charsetnr, p.length, p.type_code, p.flags, p.scale = (
            p_reader.read_struct('<xHIBHBxx'))
        # 'default' is a length coded binary and is still in the buffer?
        # not used for normal result sets...
        return p

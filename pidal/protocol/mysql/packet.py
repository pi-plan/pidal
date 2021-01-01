import struct 

import pidal.err as err

from pidal.logging import logger
from pidal.protocol.mysql import Command
from pidal.protocol.util import byte2int, dump_packet


NULL_COLUMN = 251
UNSIGNED_CHAR_COLUMN = 251
UNSIGNED_SHORT_COLUMN = 252
UNSIGNED_INT24_COLUMN = 253
UNSIGNED_INT64_COLUMN = 254


class PacketBytesReader(object):

    def __init__(self, raw: bytes):
        self._data: bytes = raw
        self._position: int = 0

    def read(self, size):
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

    def read_uint8(self):
        result = self._data[self._position]
        self._position += 1
        return result

    def read_uint16(self):
        result = struct.unpack_from('<H', self._data, self._position)[0]
        self._position += 2
        return result

    def read_uint24(self):
        low, high = struct.unpack_from('<HB', self._data, self._position)
        self._position += 3
        return low + (high << 16)

    def read_uint32(self):
        result = struct.unpack_from('<I', self._data, self._position)[0]
        self._position += 4
        return result

    def read_uint64(self):
        result = struct.unpack_from('<Q', self._data, self._position)[0]
        self._position += 8
        return result

    def read_string(self):
        end_pos = self._data.find(b'\0', self._position)
        if end_pos < 0:
            return None
        result = self._data[self._position:end_pos]
        self._position = end_pos + 1
        return result

    def read_length_encoded_integer(self):
        c = self.read_uint8()
        if c == NULL_COLUMN:
            return None
        if c < UNSIGNED_CHAR_COLUMN:
            return c
        elif c == UNSIGNED_SHORT_COLUMN:
            return self.read_uint16()
        elif c == UNSIGNED_INT24_COLUMN:
            return self.read_uint24()
        elif c == UNSIGNED_INT64_COLUMN:
            return self.read_uint64()

    def read_length_coded_string(self):
        length = self.read_length_encoded_integer()
        if length is None:
            return None
        return self.read(length)


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
        return p


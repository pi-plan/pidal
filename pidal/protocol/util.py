import struct
import sys

from typing import Union


def byte2int(b: Union[bytes, int]) -> int:
    if isinstance(b, int):
        return b
    else:
        return struct.unpack("!B", b)[0]


def int2byte(i: int) -> bytes:
    return struct.pack("!B", i)


def dump_packet(data: bytes) -> str:
    def printable(data):
        if 32 <= byte2int(data) < 127:
            if isinstance(data, int):
                return chr(data)
            return data
        return '.'
    result = '\n\n'
    try:
        result += "packet length: %s\n" % len(data)
        for i in range(1, 7):
            f = sys._getframe(i)
            result += "call[%d]: %s (line %d)\n" % (i, f.f_code.co_name,
                                                    f.f_lineno)
        result += '--------------------\n'
    except ValueError:
        pass
    dump_data = [data[i:i+16] for i in range(0, min(len(data), 256), 16)]
    for d in dump_data:
        result += (' '.join("{:02X}".format(byte2int(x)) for x in d) +
                   '   ' * (16 - len(d)) + ' ' * 2 +
                   ''.join(printable(x) for x in d))
        result += '\n'
    result += '--------------------\n'
    return result

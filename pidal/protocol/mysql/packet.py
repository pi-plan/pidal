import pidal.err as err

from pidal.logging import logger
from pidal.protocol.mysql import Command
from pidal.protocol.util import byte2int, dump_packet


class Packet(object):
    pass


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


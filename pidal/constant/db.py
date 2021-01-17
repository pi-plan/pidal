import enum


@enum.unique
class DBNodeType(enum.IntEnum):
    SOURCE = 1
    REPLICA = 2

    @classmethod
    def name2value(cls, name: str) -> 'DBNodeType':
        for member in list(cls):
            if member.name == name.upper():
                return member

        raise Exception("type [{}] is not supported.".format(name))


@enum.unique
class DBTableType(enum.IntEnum):
    RAW = 1
    SHARDING = 2
    DOUBLE_SHARDING = 3
    SYNC_TABLE = 4

    @classmethod
    def name2value(cls, name: str) -> 'DBTableType':
        for member in list(cls):
            if member.name == name.upper():
                return member

        raise Exception("type [{}] is not supported.".format(name))


@enum.unique
class SessionStatus(enum.IntFlag):
    CLOSE = 1
    INIT = 2  # 初始化
    HANDSHAKEING = 4  # 握手认证中
    HANDSHAKED = 8  # 已完成握手认证
    SERVING = 16  # 服务中
    IN_TRANSACTION = 32  # 在事务中

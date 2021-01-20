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


@enum.unique
class TransStatus(enum.IntFlag):
    INIT = 1  # 完成初始化
    BEGINNING = 2  # 事务正在启动中
    ACTIVE = 4  # 事务活动中
    COMMITING = 8  # 提交中
    ROLLBACKING = 16  # 回滚中
    END = 32  # 事务已经结束

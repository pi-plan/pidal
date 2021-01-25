import enum


@enum.unique
class A2PCMode(enum.IntEnum):
    SINGLE = 1  # 单台数据库，不分表，可以前端多个 server，后端一个数据库。
    SHARDING = 2  # 数据分片模式，可以解决单点问题。
    PERCOLATOR = 3  # Percolator 事务模型，解决数据可见性问题。

    @classmethod
    def name2value(cls, name: str) -> 'A2PCMode':
        for member in list(cls):
            if member.name == name.upper():
                return member

        raise Exception("type [{}] is not supported.".format(name))


@enum.unique
class A2PCAction(enum.IntEnum):
    ACQUIRE_LOCK = 1
    COMMIT = 2
    ROLLBACK = 3


@enum.unique
class A2PCStatus(enum.IntEnum):
    ACTIVE = 1  # 事务活跃状态
    COMMIT = 2  # 事务提交
    ROLLBACKING = 3  # 事务回滚中
    ROLLBACKED = 4  # 事务回滚完成


@enum.unique
class A2PCOperation(enum.Enum):
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    INSERT = "INSERT"

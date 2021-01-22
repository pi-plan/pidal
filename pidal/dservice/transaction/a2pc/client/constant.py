import enum


@enum.unique
class A2PCMode(enum.IntEnum):
    SINGLE = 1  # 单台数据库，不分表，可以前端多个 server，后端一个数据库。
    SHARDING = 2  # 数据分片模式，可以解决单点问题。
    PERCOLATOR = 3  # Percolator 事务模型，解决数据可见性问题。

    @classmethod
    def name2value(cls, name: str) -> 'A2PCMod':
        for member in list(cls):
            if member.name == name.upper():
                return member

        raise Exception("type [{}] is not supported.".format(name))


@enum.unique
class A2PCAction(enum.IntEnum):
    ACQUIRE_LOCK = 1
    COMMIT = 2
    ROLLBACK = 3

import enum


@enum.unique
class DBNodeType(enum.IntEnum):
    SOURCE = 1
    REPLICA = 2

    @classmethod
    def name2value(cls, name: str) -> 'DBNodeType':
        for member in list(cls):
            if member.value == name.upper():
                return member

        raise Exception("type [{}] is not supported.".format(name))


@enum.unique
class DBTableType(enum.IntEnum):
    RAW = 1
    SHARDING = 2
    DOUBLE_SHARDING = 3

    @classmethod
    def name2value(cls, name: str) -> 'DBTableType':
        for member in list(cls):
            if member.value == name.upper():
                return member

        raise Exception("type [{}] is not supported.".format(name))

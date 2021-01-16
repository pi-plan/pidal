import enum


@enum.unique
class RuleStatus(enum.IntEnum):
    BLOCK = 1
    RESHARDING = 2
    ACTIVE = 3

    @classmethod
    def name2value(cls, name: str) -> 'RuleStatus':
        for member in list(cls):
            if member.name == name.upper():
                return member

        raise Exception("type [{}] is not supported.".format(name))

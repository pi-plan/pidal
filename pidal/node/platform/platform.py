from enum import Enum


class Platform(Enum):
    SQLite = "sqlite"
    PostgreSQL = "postgresql"
    MariaDB = "mariadb"
    MySQL = "mysql"

    @classmethod
    def name2value(cls, platform: str) -> 'Platform':
        for member in list(cls):
            if member.value == platform.lower():
                return member

        raise Exception("db[{}] is not supported.".format(platform))



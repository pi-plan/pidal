from typing import Type

from pidal.node.connection import Connection
from pidal.node.platform.platform import Platform
from pidal.node.platform.mysql.aiomysql import AIOMySQL


def get_connector(platform: Platform) -> Type[Connection]:
    if platform is Platform.MySQL:
        return AIOMySQL
    elif platform is Platform.MariaDB:
        return AIOMySQL

    raise Exception("Unknown platform [{}]".format(platform))

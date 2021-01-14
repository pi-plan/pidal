import enum

from pidal.meta.manager import MetaManager


@enum.unique
class DServiceStatus(enum.IntFlag):
    INIT = 1
    SERVING = 2


class DService(object):
    def __init__(self, version: int, meta: MetaManager):
        self.version = version
        self.meta = meta
        self.status: DServiceStatus = DServiceStatus.INIT

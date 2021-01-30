import enum
from typing import List

from pidal.meta.model import ZoneConfig, DBConfig
from pidal.dservice.zone_manager import ZoneManager
from pidal.dservice.database.database import Database
from pidal.dservice.dsession import DSession


@enum.unique
class DServiceStatus(enum.IntFlag):
    INIT = 1
    SERVING = 2


class DService(object):

    def __init__(self, zone_id: int, version: int, zones: List[ZoneConfig],
                 db: DBConfig):
        self.zone_id = zone_id
        self.status: DServiceStatus = DServiceStatus.INIT
        self.version = version
        self.zone_manager: ZoneManager = ZoneManager(version, self.zone_id,
                                                     zones)
        self.db = Database(self.zone_manager, db)

    def get_version(self) -> int:
        return self.version

    def create_session(self) -> DSession:
        return DSession(self.db)

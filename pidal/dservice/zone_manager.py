from typing import List, Dict

from pidal.constant import RuleStatus
from pidal.meta.model import ZoneConfig


class ShardingID(object):
    def __init__(self, zsid: int, zone_id: int, status: RuleStatus):
        self.zsid = zsid
        self.zone_id = zone_id
        self.status = status


class ZoneManager(object):
    def __init__(self, version: int, current_zone_id: int,
                 zones: List[ZoneConfig]):
        self.version = version
        self.current_zone_id = current_zone_id
        self.zones = zones
        self.zsids: Dict[int, ShardingID] = {}
        self.current_zsids: Dict[int, ShardingID] = {}
        self._zones_to_zsids()

    def _zones_to_zsids(self):
        for zone in self.zones:
            for sharding in zone.shardings:
                zsid_item = ShardingID(sharding.zsid, zone.zone_id,
                                       sharding.status)
                self.zsids[zsid_item.zsid] = zsid_item
                if self.current_zone_id == zone.zone_id:
                    self.current_zsids[zsid_item.zsid] = zsid_item

    def is_allow(self, zsid: int) -> bool:
        sid = self.current_zsids.get(zsid, None)
        if not sid:
            return False
        return sid.status == RuleStatus.ACTIVE

    def get_pidal_c_v(self):
        return (self.current_zone_id << 53) | (self.version << 33)

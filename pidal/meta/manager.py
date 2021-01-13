from typing import Dict, List

from pidal.meta.model import ZoneConfig
from pidal.meta.client import Client


class MetaManager(object):
    def __init__(self):
        self.versions: Dict[int, ZoneConfig] = {}
        self._pimms_client: Client = Client.new()
        self.latest_version: int = 0

    def get_latest(self):
        latest_version = self._pimms_client.get_latest_version()
        if latest_version > self.latest_version:
            return
        self._pimms_client.get_zones()

    def parser_zone_config(self, conf: List[dict]):
        for i in conf:
            zone = ZoneConfig.new_from_dict(i)
            if zone.zone_id in self.zones.keys():
                raise Exception("zone id [{}] has defined.".format(
                                zone.zone_id))
            self.zones[zone.zone_id] = zone



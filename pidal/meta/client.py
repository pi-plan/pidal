import json

from typing import Any, Dict, List, Tuple, Optional

from pidal.config import Config


class Client(object):

    _instance: Optional['Client'] = None

    def __init__(self, servers: List[Tuple[str, int]], wait_timeout: int):
        self.servers = servers
        self.wait_timeout = wait_timeout

    @classmethod
    def new(cls) -> 'Client':
        if cls._instance:
            return cls._instance
        config = Config.get_instance()
        c = cls(config.get_meta_config().servers,
                config.get_meta_config().wait_timeout)
        cls._instance = c
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'Client':
        if not cls._instance:
            raise Exception("Not yet initialized")
        return cls._instance

    def _get_meta(self, version: Optional[int] = None) -> Dict[str, Any]:
        conf = json.load(open("./meta/demo.json"))
        return conf

    def get_latest_version(self) -> int:
        return 1

    def get_zones(self, version: Optional[int],
                  include_db: bool = False) -> List[Dict[str, Any]]:
        """ 获取所有的 Zone 配置 """
        return self._get_meta(version)["zones"]

    def get_zone(self, version: Optional[int], zone_id: int) -> Dict[str, Any]:
        return self.get_zones(version).get

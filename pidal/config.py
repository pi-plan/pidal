import os

from typing import List, Any, Optional, Tuple, MutableMapping

import toml


class Config(object):
    """
    所有配置的factory，单例可以被修改
    """

    _instance: Optional['Config'] = None

    def __init__(self,
                 current_zone_id: int,
                 meta_server_enable: bool,
                 zone_enable: bool):
        self.current_zone_id: int = int(current_zone_id)
        self.meta_server_enable: bool = bool(meta_server_enable)
        self.zone_enable: bool = bool(zone_enable)

    @classmethod
    def new(cls,
            current_zone_id: int,
            meta_server_enable: bool,
            zone_enable: bool):
        if cls._instance:
            del(cls._instance)
        c = cls(current_zone_id, meta_server_enable, zone_enable)
        cls._instance = c
        return c._instance

    @classmethod
    def get_instance(cls) -> 'Config':
        if not cls._instance:
            raise Exception("Not yet initialized")
        return cls._instance

    @staticmethod
    def get_proxy_config() -> 'ProxyConfig':
        return ProxyConfig.get_instance()

    @staticmethod
    def get_meta_config() -> 'MetaService':
        return MetaService.get_instance()


class MetaService(object):
    """
    单例，不能被修改
    """

    _instance: Optional['MetaService'] = None

    def __init__(self,
                 servers: List[Tuple[str, int]],
                 wait_timeout: int):
        self.servers: List[Tuple[str, int]] = servers
        self.wait_timeout: int = wait_timeout

    @classmethod
    def new(cls,
            servers: List[Tuple[str, int]],
            wait_timeout: int) -> 'MetaService':
        if cls._instance:
            return cls._instance
        c = cls(servers, wait_timeout)
        cls._instance = c
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'MetaService':
        if not cls._instance:
            raise Exception("Not yet initialized")
        return cls._instance


class ProxyConfig(object):
    """
    单例，不能被修改
    """

    _instance: Optional['ProxyConfig'] = None

    def __init__(self, host: str, port: int):
        self.host: str = host
        self.port: int = port

    @classmethod
    def new(cls, host: str, port: int) -> 'ProxyConfig':
        if cls._instance:
            return cls._instance
        c = cls(host, port)
        cls._instance = c
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'ProxyConfig':
        if not cls._instance:
            raise Exception("Not yet initialized")
        return cls._instance


class LoggingHandlerConfig(object):
    def __init__(self, class_name: str, args: List[List[Any]]):
        self.class_name: str = class_name
        self.args: List[List[Any]] = args


class LoggingConfig(object):
    """
    单例，不能被修改
    """

    _instance: Optional['LoggingConfig'] = None

    def __init__(self, level: str, format: str, datefmt: str,
                 handler: LoggingHandlerConfig):
        self.level = level
        self.format = format
        self.datefmt = datefmt
        self.handler = handler

    @classmethod
    def new(cls, level: str, format: str, datefmt: str,
            handler: LoggingHandlerConfig) -> 'LoggingConfig':
        if cls._instance:
            return cls._instance
        c = cls(level, format, datefmt, handler)
        cls._instance = c
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'LoggingConfig':
        if not cls._instance:
            raise Exception("Not yet initialized")
        return cls._instance


def parser_config(zone_id: int, file: str):
    with open(file, "r") as f:
        config = toml.load(f)
        for i in ["base"]:
            if i not in config:
                raise Exception("config file is error.")

        for i in ["proxy", "logging", "version",
                  "meta_server_enable", "zone_enable"]:
            if i not in config["base"]:
                raise Exception("config file is error.")
        for i in ["host", "port"]:
            if i not in config["base"]["proxy"]:
                raise Exception("config file is error.")

        ProxyConfig.new(config["base"]["proxy"]["host"],
                        config["base"]["proxy"]["port"])

        for i in ["level", "format", "datefmt", "handler"]:
            if i not in config["base"]["logging"]:
                raise Exception("config file is error.")

        for i in ["class", "args"]:
            if i not in config["base"]["logging"]["handler"]:
                raise Exception("config file is error.")

        logging_handler = LoggingHandlerConfig(
                config["base"]["logging"]["handler"]["class"],
                config["base"]["logging"]["handler"]["args"])
        LoggingConfig.new(
                config["base"]["logging"]["level"],
                config["base"]["logging"]["format"],
                config["base"]["logging"]["datefmt"],
                logging_handler)

        c = Config.new(_get_zone_id(zone_id, config),
                       bool(config["meta_server_enable"]),
                       bool(config["zone_enable"]))
        if c.meta_server_enable or c.zone_enable:
            if "meta_service" not in config["base"]:
                raise Exception("config file is error.")
            for i in ["servers", "wait_timeout"]:
                if i not in config["base"]["meta_service"]:
                    raise Exception("config file is error.")
            servers = []
            for i in config["base"]["meta_service"]["server"]:
                servers.append((i["host"], i["port"]))

            MetaService.new(servers,
                            config["base"]["meta_service"]["wait_timeout"])


def _get_zone_id(zone_id: int, conf: MutableMapping[str, Any]) -> int:
    """
    获取当前的 zone id 优先级为 启动参数指定 > 配置文件指定 > 环境变量
    """
    if not zone_id:
        return zone_id
    if "zone_id" in conf.keys():
        return int(conf["zone_id"])
    env_zone_id = os.environ.get("PIDAL_ZONE_ID")
    if env_zone_id:
        return int(env_zone_id)
    return 0

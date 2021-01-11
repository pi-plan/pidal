import re

from typing import List, Any, Optional, Tuple, Union

import toml


TABLE_NAME_EXP_RE = re.compile(r"^([\w.]+)_{+)\,\s*+)\,*\s*?)}$""()()()}")
TABLE_NAME_NUM_RE = re.compile(r"^([\w.]+)_(\d+)$")


class ProxyConfig(object):

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


class TableNameItem(object):
    def __init__(self, node: str, base: str, number: int):
        self.node: str = node
        self.name_base: str = base
        self.number = number


def parser_config(file: str):
    with open(file, "r") as f:
        config = toml.load(f)
        for i in ["proxy", "logging"]:
            if i not in config:
                raise Exception("config file is error.")

        for i in ["host", "port"]:
            if i not in config["proxy"]:
                raise Exception("config file is error.")

        ProxyConfig.new(config["proxy"]["host"], config["proxy"]["port"])

        for i in ["level", "format", "datefmt", "handler"]:
            if i not in config["logging"]:
                raise Exception("config file is error.")

        for i in ["class", "args"]:
            if i not in config["logging"]["handler"]:
                raise Exception("config file is error.")

        logging_handler = LoggingHandlerConfig(
                config["logging"]["handler"]["class"],
                config["logging"]["handler"]["args"])
        LoggingConfig.new(
                config["logging"]["level"],
                config["logging"]["format"],
                config["logging"]["datefmt"],
                logging_handler)


def number_expression(expression: str) -> Optional[List[TableNameItem]]:
    value = TABLE_NAME_EXP_RE.findall(expression)
    if not value:
        return None
    if len(value) > 1:
        raise Exception("expression: [{}] has two expression".format(
            expression))
    value = [i for i in value[0] if i]
    if len(value) < 3:
        raise Exception("expression: [{}] need stop.".format(
            expression))
    if len(value) > 4:
        raise Exception("expression: [{}] only need start, stop, step.".format(
            expression))
    base = str(value[0])
    start = int(value[1])
    stop = int(value[2])
    step = 1
    if len(value) > 3:
        step = int(value[3])

    result = []
    for i in range(start, stop, step):
        result.append(TableNameItem(base, i))

    return result


def parser_tablename(expression: str) -> Optional[TableNameItem]:
    value = TABLE_NAME_NUM_RE.findall(expression)
    if not value:
        return None
    if len(value) > 1:
        raise Exception("expression: [{}] has two numbers.".format(
            expression))
    value = [i for i in value[0] if i]
    if len(value) != 2:
        raise Exception("expression: [{}] need one number.".format(
            expression))
    return TableNameItem(value[0], int(value[1]))

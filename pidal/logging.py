import logging
import logging.handlers

from typing import Literal, List, Any

from pidal import NAME
from pidal.config import LoggingConfig


logger = logging.getLogger(NAME)

handler_map = {
        "NullHandler": logging.NullHandler,
        "StreamHandler": logging.StreamHandler,
        "FileHandler": logging.FileHandler,
        "RotatingFileHandler": logging.handlers.RotatingFileHandler,
        "TimedRotatingFileHandler": logging.handlers.TimedRotatingFileHandler,
        "SysLogHandler": logging.handlers.SysLogHandler,
        "HTTPHandler": logging.handlers.HTTPHandler,
        "QueueHandler": logging.handlers.QueueHandler,
        }


def init_logging(debug: bool):
    config = LoggingConfig.get_instance()
    ch = _create_handler(config.handler.class_name, config.handler.args)
    formatter = logging.Formatter(fmt=config.format, datefmt=config.datefmt)
    ch.setLevel(_name2level(config.level))
    ch.setFormatter(formatter)

    for logger in _get_logger():
        logger.setLevel(_name2level(config.level))
        logger.addHandler(ch)

    if debug:
        th = logging.StreamHandler()
        th.setLevel(logging.DEBUG)
        th.setFormatter(formatter)
        for logger in _get_logger():
            logger.addHandler(th)


def _get_logger() -> List[logging.Logger]:
    result = []
    result.append(logger)
    for i in ["tornado", "tornado.access", "tornado.application",
              "tornado.general"]:
        ilog = logging.getLogger(i)
        if not ilog:
            continue
        result.append(ilog)
    return result


def _create_handler(class_name: str, args: List[List[Any]]) -> logging.Handler:
    handler_cls = handler_map.get(class_name)
    if not handler_cls:
        raise Exception("logging config handler is unknown.")

    if args:
        args = [i[0] for i in args if i]
        handler = handler_cls(*args)
    else:
        handler = handler_cls()
    return handler


def _name2level(name: str) -> Literal[10, 20, 30, 40, 50]:
    numeric_level = getattr(logging, name.upper(), None)
    if not numeric_level:
        raise Exception("logging config level [{}]is unknown.".format(name))
    return numeric_level

import argparse


from pidal import VERSION
from pidal.config import parser_config
from pidal.logging import init_logging
from pidal.proxy import ProxyServer


def parse_args():
    parser = argparse.ArgumentParser(description="welcome use PiDAL")
    parser.add_argument("--debug", action="store_true",
                        default=True,  # TODO remove when 1.0.0
                        help="open debug mode.")
    parser.add_argument('--version', action='version', version=VERSION)
    parser.add_argument("--config", metavar="conf.toml", action="store",
                        default="conf.toml", help="config file path.")
    args = parser.parse_args()
    return args


def main():
    args = vars(parse_args())
    debug = args["debug"]
    parser_config(args["config"])
    init_logging(debug)
    server = ProxyServer()
    server.start()

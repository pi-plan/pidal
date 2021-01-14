import argparse


from pidal import VERSION
from pidal.config import parser_config
from pidal.logging import init_logging
from pidal.frontend.frontend import Frontend


def parse_args():
    parser = argparse.ArgumentParser(description="welcome use PiDAL")
    parser.add_argument("--debug", action="store_true",
                        default=True,  # TODO remove when 1.0.0
                        help="open debug mode.")
    parser.add_argument('--version', action='version', version=VERSION)
    parser.add_argument("--config", metavar="conf.toml", action="store",
                        default="conf.toml", help="config file path.")
    parser.add_argument("--zone-id", metavar="1", action="store", type=int,
                        default=0, help="current zone id.")
    args = parser.parse_args()
    return args


def main():
    args = vars(parse_args())
    debug = args["debug"]
    parser_config(args["zone_id"], args["config"])
    init_logging(debug)
    server = Frontend()
    server.start()

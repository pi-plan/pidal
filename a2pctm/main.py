import argparse


from pidal import VERSION
from pidal.config import parser_config
from pidal.logging import init_logging


def parse_args():
    parser = argparse.ArgumentParser(description="welcome use PiDAL")
    parser.add_argument("--debug", action="store_true",
                        default=True,  # TODO remove when 1.0.0
                        help="open debug mode.")
    parser.add_argument('--version', action='version', version=VERSION)
    parser.add_argument("--config", metavar="a2pc.toml", action="store",
                        default="a2pc.toml", help="config file path.")
    parser.add_argument("--zone-id", metavar="1", action="store", type=int,
                        default=0, help="current zone id.")
    args = parser.parse_args()
    return args


async def main():
    args = vars(parse_args())
    debug = args["debug"]
    parser_config(args["zone_id"], args["config"])
    init_logging(debug)

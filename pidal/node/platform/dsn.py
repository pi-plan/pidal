from urllib.parse import urlparse, parse_qs
from typing import Any, List, Dict, Optional

from pidal.node.platform.platform import Platform


class DSN(object):

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.platform: Platform
        self.hostname: Optional[str]
        self.port: Optional[int]
        self.username: Optional[str]
        self.password: Optional[str]
        self.database: Optional[str]
        self.max_idle_time: Optional[float] = 3600  # TODO be config.
        self.args: Optional[Dict[str, Any]] = {}
        self.parse_dsn()

    def parse_dsn(self):
        url_info = urlparse(self.dsn)
        self.parse_scheme(url_info.scheme)

        self.hostname = url_info.hostname
        self.port = url_info.port
        self.username = url_info.username
        self.password = url_info.password

        self.parse_path(url_info.path)
        self.parse_query(url_info.query)

    def parse_path(self, path: str):
        self.database = path.lstrip("/")

    def parse_query(self, query: str):
        if not query:
            return

        args = parse_qs(query)
        for k, v in args.items():
            self.args[k] = self.args_type_conver(v)

    def parse_scheme(self, scheme: str):
        scheme_info = scheme.split("+")
        self.platform = Platform.name2value(scheme_info[0])

    def args_type_conver(self, values: List[str]) -> object:
        value = values[0]
        if value in ["True", "False", "None", "null"]:
            value = self.string2bool(value)
        elif value.isdigit():
            value = int(value)
        return value

    def get_args(self) -> Dict[str, Any]:
        if self.platform is Platform.MySQL:
            return self._get_pymysql_args()
        raise Exception("unkonwn db driver.")

    def _get_sqlite_args(self):
        args: Dict[str, Any] = dict()
        args['database'] = self.database
        if self.args is not None:
            args.update(self.args)
        return args

    def _get_pymysql_args(self):
        args = dict()
        args["host"] = self.hostname
        args["port"] = self.port
        args["user"] = self.username
        args["password"] = self.password
        args["db"] = self.database
        if self.args is not None:
            args.update(self.args)
        return args

    def _get_mysqlclient_args(self):
        args = dict()
        args["host"] = self.hostname
        args["port"] = self.port
        args["user"] = self.username
        args["passwd"] = self.password
        args["db"] = self.database
        if self.args is not None:
            args.update(self.args)
        return args

    def _get_psycopg_args(self):
        args = dict()
        args["host"] = self.hostname
        args["port"] = self.port
        args["user"] = self.username
        args["password"] = self.password
        args["database"] = self.database
        if self.args is not None:
            args.update(self.args)
        return args

    @staticmethod
    def string2bool(v: str) -> Optional[bool]:
        if v == "False":
            return False
        elif v == "True":
            return True
        elif v == "None":
            return None

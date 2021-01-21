from typing import Any, Dict, List, Optional
from pidal.node.result import result
from pidal.node.connection import Connection


class Tools(object):

    @staticmethod
    async def get_lock_columns(conn: Connection,
                               table: str, key: str) -> List[str]:
        sql = "SHOW KEYS FROM {} WHERE Non_unique = 0 and Key_name = \"{}\"".\
                format(table, key)
        r = await conn.query(sql)
        seq_in_index_i: Optional[int] = None
        column_name_i: Optional[int] = None
        if isinstance(r, result.ResultSet):
            if not r or not r.rows:
                raise Exception("unkonwn table [{}] key [{}]".format(
                    table, key))
            for i, d in enumerate(r.descriptions):
                if d.name == "Seq_in_index":
                    seq_in_index_i = i
                if d.name == "Column_name":
                    column_name_i = i
        else:
            raise Exception("can not get table [{}] key [{}]".format(table,
                            key))

        if not seq_in_index_i or not column_name_i:
            raise Exception("unkonwn table [{}] key [{}]".format(table, key))

        result = {}
        for i in r.rows:
            result[i[seq_in_index_i]] = i[column_name_i]
        return [result[k] for k in sorted(result.keys())]

    @staticmethod
    async def get_column_default(conn: Connection, table: str) -> \
            Dict[str, Any]:
        sql = "SHOW COLUMNS FROM {}".format(table)
        r = await conn.query(sql)
        field_i = None
        default_i = None
        if isinstance(r, result.ResultSet):
            if not r or not r.rows:
                raise Exception("unkonwn table [{}] column default.".format(
                                table))
            for i, d in enumerate(r.descriptions):
                if d.name == "Field":
                    field_i = i
                if d.name == "Default":
                    default_i = i
        else:
            raise Exception("can not get table [{}] column default.".format(
                            table))

        if not field_i or not default_i:
            raise Exception("unkonwn table [{}] column default.".format(table))

        result = {}
        for i in r.rows:
            result[i[field_i]] = i[default_i]
        return result



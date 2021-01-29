from typing import List, Optional, Dict

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison, Token,\
        Parenthesis, Function, Values
import sqlparse.tokens as token
from sqlparse.sql import Statement


class SQL(object):
    def __init__(self, raw: Statement):
        self.raw = raw
        self.raw_where = {}
        self.new_value = {}

    def has_table(self) -> bool:
        return hasattr(self, "table")

    def add_pidal(self, _: int):  # type: ignore
        pass

    def parse_table_name(self):
        fl = self._get_from_part()
        if not fl:
            return
        table = self.extract_table_identifiers(fl)
        if len(table) != 1:
            raise Exception("Not support sub select.")
        self.table = table[0]

    def parse_where(self, where) -> Dict[str, int]:
        column = {}
        for i in where[1:]:
            if isinstance(i, Token) and i.ttype is token.Whitespace:
                continue
            if isinstance(i, Comparison):
                r = self._parse_comparison(i)
                if r:
                    column[r[0]] = r[1]
                continue
            elif isinstance(i, Parenthesis):
                column.update(self.parse_where(i.tokens))
                continue
        return column

    def modify_table(self, name: str):
        self.table[0].value = name

    def _parse_comparison(self, s: Comparison):
        column = None
        for i in s.tokens:
            if isinstance(i, Token) and i.ttype is token.Whitespace:
                continue
            if not column:
                if isinstance(i, Identifier):
                    column = i.value
                continue

            if i.ttype is token.Comparison:
                if i.value != "=":
                    return
            self.raw_where[column] = i.value
            if i.ttype in token.Number:
                return (column, i.value)

    def _get_where_part(self):
        for item in self.raw.tokens:
            if item.is_group and isinstance(item, Where):
                return item

    def _get_from_part(self):
        from_seen = False
        for i, item in enumerate(self.raw.tokens):
            if from_seen:
                if self.is_subselect(item):
                    raise Exception("Not support sub select.")
                else:
                    continue
            elif item.ttype is token.Keyword and item.value.upper() == "FROM":
                from_seen = True
                return self.raw.tokens[i + 1:]

    @staticmethod
    def is_subselect(parsed):
        if not parsed.is_group:
            return False
        for item in parsed.tokens:
            if item.ttype is token.DML and item.value.upper() == 'SELECT':
                return True
        return False

    @staticmethod
    def extract_table_identifiers(token_stream):
        tables = []
        for item in token_stream:
            if isinstance(item, Where):
                break
            elif isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    tables.append(identifier)
            elif isinstance(item, Identifier):
                tables.append(item)
        return tables


class Other(SQL):

    def __init__(self, raw: Statement):
        self.raw = raw


class TCL(SQL):
    def __init__(self, raw: Statement):
        self.is_commit: bool = False
        self.is_rollback: bool = False
        self.is_start: bool = False
        self.trans_args: Optional[List[str]] = None

        self.raw = raw
        self.parse()

    def modify_table(self, name: str):  # type:ignore
        pass

    def parse(self):
        get_first = False
        for i in self.raw.tokens:
            if i.ttype.parent is token.Text or i.value == "TRANSACTION" or\
                    i.ttype is token.Punctuation:
                continue
            if not get_first:
                if i.value == "COMMIT":
                    self.is_commit = True
                    return
                elif i.value == "ROLLBACK":
                    self.is_rollback = True
                    return
                elif i.value in ("START", "BEGIN"):
                    self.is_start = True
                    get_first = True
                    self.trans_args = []
                    continue
                else:
                    raise Exception("unkonwn transaction")
            self.trans_args.append(i.value)


class DML(SQL):

    def __init__(self, raw: Statement):
        self.raw = raw
        self.table: Identifier
        self.column: Dict[str, int]


class Select(DML):

    def __init__(self, raw: Statement):
        self.table_name: str
        self.column: Dict[str, int]
        self.for_update: bool = False
        self.raw_where = {}
        self.new_value = {}

        self.raw = raw
        self.parse()

    def parse(self):
        self.parse_table_name()
        where = self._get_where_part()
        if not where:
            return
        self.column = self.parse_where(where)
        self._parse_for_update(where)

    def _parse_for_update(self, where):
        tokens = []
        for v in where:
            if v.ttype in (token.Keyword, token.DML):
                tokens.append(v)
        if len(tokens) < 2:
            self.for_update = False
            return
        if tokens[-1].value == "UPDATE" and tokens[-2].value == "FOR":
            self.for_update = True

    def is_for_update(self) -> bool:
        return self.for_update


class DMLW(DML):
    pass


class Delete(DMLW):

    def __init__(self, raw: Statement):
        self.table_name: str
        self.column: Dict[str, int]
        self.raw_where = {}
        self.new_value = {}

        self.raw = raw
        self.parse()

    def parse(self):
        self.parse_table_name()
        where = self._get_where_part()
        self.column = self.parse_where(where)

    def get_where(self):
        return self._get_where_part()

    def add_pidal(self, _: int):  # type: ignore
        for item in self.raw.tokens:
            if item.is_group and isinstance(item, Where):
                c = sqlparse.parse(" pidal_c & 1 AND ")
                c[0].tokens.reverse()
                for v in c[0].tokens:
                    item.insert_before(1, v)


class Update(DMLW):

    def __init__(self, raw: Statement):
        self.table_name: str
        self.column: Dict[str, int]
        self.raw_where = {}
        self.new_value = {}

        self.raw = raw
        self.parse()

    def parse(self):
        self.parse_table_name()
        where = self._get_where_part()
        self.column = self.parse_where(where)
        self.get_where()

    def get_where(self):
        where = self._get_where_part()
        for i in where.tokens:
            if i.ttype == token.Punctuation and i.value == ";":
                index = where.tokens.index(i)
                where.tokens = where.tokens[:index]
        return where

    def parse_table_name(self):
        fl = self._get_from_part()
        table = self.extract_table_identifiers(fl)
        self.table = table[0]
        self._parse_new_values(table[1:])

    def _parse_new_values(self, steam):
        for i in steam:
            new_set = []
            for j in i.tokens:
                if j.ttype is token.Whitespace:
                    continue
                new_set.append(j)

            if not isinstance(new_set[0], Identifier) or \
                    new_set[1].value != "=":
                raise Exception("pidal  not support sql.")
            self.new_value[new_set[0].value] = str(new_set[2]).strip("'\"")

    def _get_from_part(self):
        for i, item in enumerate(self.raw.tokens):
            if item.ttype is token.DML and item.value.upper() == "UPDATE":
                return self.raw.tokens[i + 1:]

    @staticmethod
    def extract_table_identifiers(token_stream):
        tables = []
        for item in token_stream:
            if isinstance(item, Where):
                break
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    tables.append(identifier)
            elif isinstance(item, Identifier):
                tables.append(item)
            elif isinstance(item, Comparison):
                tables.append(item)
        return tables

    def add_pidal(self, value: int):
        where = None
        for i, item in enumerate(self.raw.tokens):
            if item.is_group and isinstance(item, Where):
                where = item
                c = sqlparse.parse(" pidal_c & 1 AND ")
                c[0].tokens.reverse()
                for i, v in enumerate(c[0].tokens):
                    item.insert_before(1, v)
        pidal_c = ", `pidal_c` = {}| CONV(RIGHT(BIN(`pidal_c`), 21), 2, 10)+2 "
        for i in sqlparse.parse(pidal_c.format(value))[0].tokens:
            self.raw.insert_before(where, i)


class Insert(DMLW):

    def __init__(self, raw: Statement):
        self.table_name: str
        self.column: Dict[str, int]
        self.raw_where = {}
        self.new_value = {}

        self.table_f: Function
        self.values: Values

        self.raw = raw
        self.parse()

    def parse(self):
        fl = self._get_from_part()
        for i in fl:
            if i.ttype is token.Whitespace:
                continue
            if isinstance(i, Function):
                self.table_f = i
            elif isinstance(i, Values):
                self.values = i
        if not self.table_f or not self.values:
            raise Exception("sql error.")

        column = self._parse_table(self.table_f)
        if not column:
            raise Exception("sql error.")

        values = self._parse_values(self.values, column)
        if not values:
            return
        self.column = {}
        for k, v in values.items():
            self.column[k] = v
        self.new_value = values

    def add_pidal(self, value: int):
        for i in self.table_f.tokens:
            if isinstance(i, Parenthesis):
                for j in i.tokens:
                    if isinstance(j, IdentifierList):
                        j.insert_after(len(j.tokens),
                                       Token(token.Punctuation, ","))
                        j.insert_after(len(j.tokens),
                                       sqlparse.parse("pidal_c")[0].tokens[0])

        for i in self.values.tokens:
            if isinstance(i, Parenthesis):
                for j in i.tokens:
                    if isinstance(j, IdentifierList):
                        j.insert_after(len(j.tokens),
                                       Token(token.Punctuation, ","))
                        j.insert_after(len(j.tokens),
                                       Token(token.Number.Integer, value))

    def _parse_table(self, table: Function):
        assert isinstance(table, Function)
        column = []
        for i in table:
            if isinstance(i, Identifier):
                self.table = i
            if isinstance(i, Parenthesis):
                for j in i.tokens:
                    if isinstance(j, IdentifierList):
                        for k in j.tokens:
                            if isinstance(k, Identifier):
                                column.append(k.value)

        return column

    @staticmethod
    def _parse_values(values: Values, column: List[str]):
        assert isinstance(values, Values)
        value = None
        for i in values.tokens:
            if isinstance(i, Parenthesis):
                if value:
                    raise Exception("only insert one.")
                for j in i.tokens:
                    if isinstance(j, IdentifierList):
                        index = 0
                        value = {}
                        for k in j.tokens:
                            t = str(k.value).strip("\"'")
                            if not t:
                                continue
                            if t == ",":
                                index += 1
                            else:
                                value[column[index]] = t
        return value

    def _get_from_part(self):
        has_seen = False
        for i, item in enumerate(self.raw.tokens):
            if item.ttype is token.DML and item.value.upper() == "INSERT":
                has_seen = True
                continue
            if has_seen and item.ttype is token.Whitespace:
                continue
            if has_seen:
                if item.value.upper() == "INTO":
                    return self.raw.tokens[i + 1:]
                else:
                    return self.raw.tokens[i:]


class Parser(object):

    ignore_tokens = [
            token.Text
            ]

    @classmethod
    def parse(cls, sql: str) -> List[SQL]:
        sql = sqlparse.format(sql, keyword_case='upper', strip_whitespace=True)
        statements = sqlparse.parse(sql)
        result = []
        for s in statements:
            s_type = s.get_type()
            if s_type == "SELECT":
                c = cls.to_select(s)
            elif s_type == "INSERT":
                c = cls.to_insert(s)
            elif s_type == "UPDATE":
                c = cls.to_update(s)
            elif s_type == "DELETE":
                c = cls.to_delete(s)
            elif s_type in ("START", "COMMIT", "ROLLBACK"):
                c = cls.to_tcl(s)
            else:
                c = cls.to_other(s)
            result.append(c)

        return result

    @staticmethod
    def to_select(s: Statement) -> SQL:
        return Select(s)

    @staticmethod
    def to_update(s: Statement) -> SQL:
        return Update(s)

    @staticmethod
    def to_insert(s: Statement) -> SQL:
        return Insert(s)

    @staticmethod
    def to_delete(s: Statement) -> SQL:
        return Delete(s)

    @staticmethod
    def to_tcl(s: Statement) -> SQL:
        return TCL(s)

    @classmethod
    def to_other(cls, s: Statement) -> SQL:
        if str(s[0]).upper() == "BEGIN":
            return cls.to_tcl(s)
        else:
            return Other(s)


if __name__ == "__main__":

    sql = """
    begin read only;
    start transaction read only;
    commit;
    rollback;
    select id, name from a where id = 3 and (age = 12) and ( id >= 1 );
    select * from a where id = 3 for update;
    update a set `name` = "dd", where id = 1;
    update a set `name` = "dd", age = 23, v = `v` + 1 where id = 1;
    insert into a (id, name) values (1, "ff");
    delete from a where id = 3 and status = 0
    """

    de = Parser.parse(sql)
    for i in de:
        if not i.has_table():
            continue
        i.add_pidal(100)
        i.modify_table("a_22")

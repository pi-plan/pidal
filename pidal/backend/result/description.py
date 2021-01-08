from typing import Optional


class Description(object):

    def __init__(self,
                 catalog: Optional[str],
                 db: Optional[str],
                 table_name: Optional[str],
                 org_table: Optional[str],
                 name: Optional[str],
                 org_name: Optional[str],
                 charsetnr: int,
                 length: int,
                 type_code: int,
                 flags: int,
                 scale: int):
        self.catalog: Optional[str] = catalog
        self.db: Optional[str] = db
        self.table_name: Optional[str] = table_name
        self.org_table: Optional[str] = org_table
        self.name: Optional[str] = name
        self.org_name: Optional[str] = org_name
        self.charsetnr: int = charsetnr
        self.length: int = length
        self.type_code: int = type_code
        self.flags: int = flags
        self.scale: int = scale

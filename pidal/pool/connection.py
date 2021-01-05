import aiomysql.connection as mysql


class Connection(object):

    def __init__(self, conn: mysql.Connection):
        self.conn: mysql.Connection = conn

    def query(self, sql: str):
        return self.conn.query(sql)

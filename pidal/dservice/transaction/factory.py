from pidal.dservice.transaction.a2pc.a2pc import A2PC
from pidal.dservice.database.database import Database
from pidal.dservice.transaction.simple import Simple
from pidal.dservice.transaction.trans import Trans


class TransFactory(object):
    trans_map = {
            "simple": Simple,
            "a2pc": A2PC,
            }

    @classmethod
    def new(cls, trans: str, db: Database, *args: str) -> Trans:
        t = cls.trans_map.get(trans, None)
        if not t:
            raise Exception("unkonwn transaction {}.".format(trans))
        return t.new(db, *args)

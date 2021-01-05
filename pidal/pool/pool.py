import aiomysql

from typing import Optional

from pidal.pool.connection import Connection


class Pool(object):

    instance: 'Pool' = None

    @classmethod
    def new(cls, host='127.0.0.1', port=3306, user='root', password=''):
        pool = aiomysql.create_pool(minsize=1, maxsize=10, host=host,
                                    port=port, user=user, password=password)
        p = cls()
        p.pool = pool
        cls.instance = p
        return p

    async def acquire(cls) -> Connection:
        return await cls.instance.acquire()

    def __init__(self):
        self.pool: 'Pool'


class PoolManager(object):

    pool: 'Pool' = Optional[Pool]

    @classmethod
    def new(cls, host='127.0.0.1', port=3306, user='root', password=''):
        pool = aiomysql.create_pool(minsize=1, maxsize=10, host=host,
                                    port=port, user=user, password=password)
        cls.pool = pool
        return pool

    @classmethod
    async def acquire(cls) -> Connection:
        return await cls.pool.acquire()

    def __init__(self):
        self.pool: 'Pool'


PoolManager.new()

import asyncpg
from asyncpg import Connection
from asyncpg.pool import Pool
from typing import Optional, List, Dict


class Database:
    def __init__(self):
        self.pool = None

    async def init(self):
        pass

    async def get_actor_id(self, actor_name: str) -> int:
        raise NotImplementedError(self.get_actor_id.__name__)

    async def get_actor_ids(self, actor_names: List[str]) -> Dict[str, int]:
        raise NotImplementedError(self.get_actor_ids.__name__)

    async def get_actor_names(self, actor_ids: List[int]) -> Dict[int, str]:
        raise NotImplementedError(self.get_actor_names.__name__)

    async def get_actor_pairs(self):
        async with self.pool.acquire() as conn:     # type: Connection
            async with conn.transaction():
                async for row in conn.cursor('select id1, id2 from peers where id1 < id2'):
                    yield row

    async def table_exists(self, table_name: str) -> bool:
        async with self.pool.acquire() as conn:     # type: Connection
            result = await conn.fetch('select 1 from information_schema.tables where table_name = $1', table_name)
        return len(result) > 0

    async def close(self):
        pass


class DatabaseOnline(Database):
    def __init__(self, dsn: str, username: str, password: str):
        super().__init__()
        self.dsn = dsn
        self.user = username
        self.pasword = password
        self.pool = None    # type: Optional[Pool]

    async def init(self):
        self.pool = await asyncpg.create_pool(self.dsn, user=self.user, password=self.pasword)

    async def get_actor_id(self, actor_name: str) -> int:
        async with self.pool.acquire() as conn:     # type: Connection
            return await conn.fetchval('select id from actors where name = $1', actor_name)

    async def get_actor_ids(self, actor_names: List[str]) -> Dict[str, int]:
        async with self.pool.acquire() as conn:     # type: Connection
            result = await conn.fetch('select id, name from actors where name = any($1)', actor_names)

        return {row[1]: row[0] for row in result}

    async def get_actor_names(self, actor_ids: List[int]) -> Dict[int, str]:
        async with self.pool.acquire() as conn:     # type: Connection
            result = await conn.fetch('select id, name from actors where id = any($1)', actor_ids)

        return {row[0]: row[1] for row in result}

    async def close(self):
        await self.pool.close()


class DatabaseMemoryCached(DatabaseOnline):
    def __init__(self, dsn: str, username: str, password: str):
        super().__init__(dsn, username, password)
        self.actor_id_to_name = {}      # Dict[int, str]
        self.actor_name_to_id = {}      # Dict[str, int]

    async def init(self):
        await super().init()
        async for id_, name in self.get_all_actors():
            self.actor_id_to_name[id_] = name
        self.actor_name_to_id = {v: k for k, v in self.actor_id_to_name.items()}

    async def get_all_actors(self):
        async with self.pool.acquire() as conn:  # type: Connection
            async with conn.transaction():
                async for row in conn.cursor('select id, name from actors'):
                    yield row

    async def get_actor_id(self, actor_name: str) -> int:
        return self.actor_name_to_id.get(actor_name)

    async def get_actor_ids(self, actor_names: List[str]) -> Dict[str, int]:
        return {a: self.actor_name_to_id[a] for a in actor_names if a in self.actor_name_to_id}

    async def get_actor_names(self, actor_ids: List[int]) -> Dict[int, str]:
        return {i: self.actor_id_to_name[i] for i in actor_ids if i in self.actor_id_to_name}

import asyncio
import pytest
from typing import Dict
from service.config import DB_DSN, DB_USER, DB_PASSWORD
import asyncpg as apg
from service.backend import DatabaseOnline


@pytest.fixture(scope='module')
async def conn():
    conn = await apg.connect(DB_DSN, user=DB_USER, password=DB_PASSWORD)
    yield conn
    await conn.close()


@pytest.fixture(scope='module')
async def db():
    db = DatabaseOnline(DB_DSN, username=DB_USER, password=DB_PASSWORD)
    await db.init()
    yield db
    await db.close()


@pytest.yield_fixture(scope='session')
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


async def get_some_random_actors(conn: apg.Connection, count: int) -> Dict[int, str]:
    result = await conn.fetch('select id, name from actors order by random() limit $1', count)
    return dict(result)


@pytest.mark.asyncio
async def test_get_actor_id(conn: apg.Connection, db: DatabaseOnline):
    rnd_actor = await get_some_random_actors(conn, 1)
    test_id, test_name = list(rnd_actor.items())[0]
    assert await db.get_actor_id(test_name) == test_id


@pytest.mark.asyncio
async def test_get_actor_ids(conn: apg.Connection, db: DatabaseOnline):
    rnd_actors = await get_some_random_actors(conn, 10)
    ids = await db.get_actor_ids(list(rnd_actors.values()))

    for id_, name in rnd_actors.items():
        assert ids[name] == id_


@pytest.mark.asyncio
async def test_get_actor_names(conn: apg.Connection, db: DatabaseOnline):
    rnd_actors = await get_some_random_actors(conn, 10)
    names = await db.get_actor_names(list(rnd_actors.keys()))

    for id_, name in rnd_actors.items():
        assert names[id_] == name


if __name__ == '__main__':
    pytest.main(args=['--disable-warnings'])

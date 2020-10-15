import os
import asyncio
import aiohttp
import pytest
import asyncpg as apg
from typing import Dict
from urllib.parse import urljoin, quote_plus
from service.config import DB_DSN, DB_USER, DB_PASSWORD


api_url = os.getenv('API_URL', 'http://localhost:8080')


@pytest.fixture(scope='module')
async def conn():
    conn = await apg.connect(DB_DSN, user=DB_USER, password=DB_PASSWORD)
    yield conn
    await conn.close()


@pytest.yield_fixture(scope='session')
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


async def get_some_random_bn(conn: apg.Connection, count: int) -> Dict[str, int]:
    result = await conn.fetch('''
        select a.name, bn.bn
        from actors a
        join bacon_numbers bn on a.id = bn.actor_id
        order by random()
        limit $1
    ''', count)
    return dict(result)


@pytest.mark.asyncio
async def test_some_random_bn(conn: apg.Connection):
    rnd_actors = await get_some_random_bn(conn, 100)
    url = urljoin(api_url, '/bn')
    for actor_name, bn in rnd_actors.items():
        async with aiohttp.ClientSession() as session:
            async with session.get(url + f'?name={quote_plus(actor_name)}') as response:
                assert response.status == 200
                result = await response.json()
                assert result['dist'] == bn


if __name__ == '__main__':
    pytest.main(args=['--disable-warnings'])

import os
import time
import asyncio
import aiohttp
from urllib.parse import urljoin, quote_plus
from typing import List
import asyncpg as apg
from service.config import DB_DSN, DB_USER, DB_PASSWORD


api_url = os.getenv('API_URL', 'http://localhost:8080')
num_requests = 5000
counter = 0
parallel_tasks = 100
actor_names = None


async def main():
    db_conn = await apg.connect(DB_DSN, user=DB_USER, password=DB_PASSWORD)
    await bechmark_bn(db_conn)
    await bechmark_dist(db_conn)


async def bechmark_bn(db_conn):
    global counter, actor_names
    counter = 0
    actor_names = await get_some_random_actors(db_conn, num_requests)

    start = time.monotonic()
    await multiply(request_bn, parallel_tasks)
    end = time.monotonic()
    dur = end - start
    rate = counter / dur
    print(f'{counter} Bacon Numbers calculated in {round(dur, 1)} ({round(rate)}/s)')


async def bechmark_dist(db_conn):
    global counter, actor_names
    counter = 0
    actor_names = await get_some_random_actors(db_conn, num_requests * 2)

    start = time.monotonic()
    await multiply(request_bn, parallel_tasks)
    end = time.monotonic()
    dur = end - start
    rate = counter / dur
    print(f'{counter} random pair distances calculated in {round(dur, 1)} ({round(rate)}/s)')


async def multiply(coroutine, number: int):
    tasks = []
    for _ in range(number):
        tasks.append(asyncio.create_task(coroutine()))
    await asyncio.gather(*tasks)


async def get_some_random_actors(conn: apg.Connection, count: int) -> List[str]:
    result = await conn.fetch('select name from actors order by random() limit $1', count)
    print(f'Got {count} random actors')
    return [r[0] for r in result]


async def request_bn():
    global counter
    url = urljoin(api_url, '/bn')
    async with aiohttp.ClientSession() as session:
        while counter < num_requests:
            name = actor_names[counter]
            counter += 1
            async with session.get(url + f'?name={quote_plus(name)}') as response:
                assert response.status == 200
                await response.read()


async def request_dist():
    global counter
    url = urljoin(api_url, '/bn')
    while counter < num_requests:
        async with aiohttp.ClientSession() as session:
            name1 = actor_names[counter]
            name2 = actor_names[counter + 1]
            counter += 2
            async with session.get(url + f'?name1={quote_plus(name1)}&name2={quote_plus(name2)}') as response:
                assert response.status == 200
                await response.read()


if __name__ == '__main__':
    asyncio.run(main())

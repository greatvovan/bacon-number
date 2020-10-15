"""
Transforms the data into relational tables.
Data is parsed from CSV files and loaded into Postgres.
"""

import os
import csv
import asyncio
import asyncpg
from typing import List, Dict, Any


DB_DSN = os.getenv('DB_DSN', 'postgres://postgres@localhost/postgres')
MOVIES_CSV_PATH = 'dataset/movies_metadata.csv'
ACTORS_CSV_PATH = 'dataset/credits.csv'
actors_lookup = {}  # type: Dict[str, int]


async def init_db(db: asyncpg.Connection):
    await db.execute('DROP TABLE IF EXISTS cast_data')
    await db.execute('DROP TABLE IF EXISTS peers')
    await db.execute('DROP TABLE IF EXISTS bacon_numbers')
    await db.execute('DROP TABLE IF EXISTS movies')
    await db.execute('DROP TABLE IF EXISTS actors')
    await db.execute('CREATE TABLE movies (id INTEGER NOT NULL, name TEXT NOT NULL)')
    await db.execute('CREATE TABLE actors (id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL)')
    await db.execute('CREATE TABLE cast_data (movie_id INTEGER NOT NULL, actor_id INTEGER NOT NULL)')
    await db.execute('CREATE TABLE peers (id1 INTEGER NOT NULL, id2 INTEGER NOT NULL)')
    await db.execute('CREATE TABLE bacon_numbers (actor_id INTEGER NOT NULL, bn SMALLINT NOT NULL, '
                     'PRIMARY KEY (actor_id))')


async def import_movies(fpath: str, db: asyncpg.Connection):
    print('Importing movies from the dataset...')
    counter = 0
    met = set()
    with open(fpath, 'rt', newline='') as input_csv, open('movies.csv', 'wt', newline='') as out_csv:
        input_csv.readline()
        csv_reader = csv.reader(input_csv)
        csv_writer = csv.writer(out_csv, delimiter=',')
        for row in csv_reader:
            id_ = row[5]
            name = row[8]
            try:
                iid = int(id_)
                if iid in met:
                    print(f'Duplicate movie ID: {id_} ({name}), skipping...')
                else:
                    csv_writer.writerow((iid, name))
                    met.add(iid)
                    counter += 1
            except ValueError:                  # Unquoted string
                print(f'Unexpected value for ID: {id_}, skipping...')

    await db.copy_to_table('movies', source='movies.csv', format='csv', delimiter=',')
    print(counter, 'movies imported')


async def import_actors(fpath: str, db: asyncpg.Connection):
    print('Importing cast data from the dataset...')
    counter = 0
    with open(fpath, 'rt', newline='') as input_csv, open('actors.csv', 'wt', newline='') as actors_csv,\
            open('cast_data.csv', 'wt', newline='') as cast_csv:
        input_csv.readline()
        csv_reader = csv.reader(input_csv)
        actors_writer = csv.writer(actors_csv, delimiter=',')
        cast_writer = csv.writer(cast_csv, delimiter=',')
        for row in csv_reader:
            cast = eval(row[0])         # Crazy!
            movie_id = int(row[2])
            load_cast(movie_id, cast, actors_writer, cast_writer)
            counter += 1
            if counter % 10000 == 0:
                print(counter, 'rows processed')
    print(counter, 'rows processed')
    await db.copy_to_table('actors', source='actors.csv', format='csv', delimiter=',')
    await db.copy_to_table('cast_data', source='cast_data.csv', format='csv', delimiter=',')
    print(len(actors_lookup), 'actors imported')


def load_cast(movie_id: int, cast: List[Dict[str, Any]], actors_writer, cast_writer):
    for actor in cast:
        actor_id = upsert_actor(actor['name'], actors_writer)
        cast_writer.writerow((movie_id, actor_id))


def upsert_actor(name: str, actors_writer) -> int:
    actor_id = actors_lookup.get(name)
    if actor_id is not None:
        return actor_id

    actor_id = len(actors_lookup) + 1
    actors_writer.writerow((actor_id, name))
    actors_lookup[name] = actor_id
    return actor_id


async def create_indices(db: asyncpg.Connection):
    print('Creating indices...')
    await db.execute('alter table movies add primary key (id)')
    await db.execute('alter table actors add primary key (id)')
    await db.execute('create index on cast_data (actor_id, movie_id)')
    await db.execute('create index on cast_data (movie_id, actor_id)')
    await db.execute('create unique index on actors (name, id)')


async def calculate_pairs(db: asyncpg.Connection):
    print('Generating pairs...')
    query = '''
        insert into peers
        select distinct a1.id, a2.id
        from actors a1
        join cast_data c1 on a1.id = c1.actor_id
        --join movies m on c1.movie_id = m.id
        --join cast_data c2 on m.id = c2.movie_id
        join cast_data c2 on c1.movie_id = c2.movie_id
        join actors a2 on c2.actor_id = a2.id
        where a1.id != a2.id
    '''
    await db.execute(query)
    print('Indexing...')
    await db.execute('CREATE INDEX ON peers (id1, id2)')


async def calculate_bacon(db: asyncpg.Connection):
    print('Calculating Bacon numbers (dubug purposes, not used in API)...')
    bacon_id = await db.fetchval("select id from actors where name = 'Kevin Bacon'")
    await db.execute('insert into bacon_numbers values ($1, 0)', bacon_id)

    add_level_query = '''
        insert into bacon_numbers
        select id2, $1::smallint
        from (
            select distinct id2
            from bacon_numbers bn
            join peers p on bn.actor_id = p.id1
            where bn.bn = $1 - 1
        ) v
        where not exists (select 1 from bacon_numbers where actor_id = id2);
    '''

    cur_level = 1
    while True:
        print('Level', cur_level)
        result = await db.execute(add_level_query, cur_level)
        num_rows = int(result.split()[-1])
        if num_rows == 0:
            break
        cur_level += 1

    print('Level', -1)
    await db.execute('''
        insert into bacon_numbers
        select id, -1
        from actors
        where not exists (select 1 from bacon_numbers where actor_id = id);
    ''')

    print('Indexing...')
    await db.execute('create unique index on bacon_numbers(actor_id, bn)')


async def main():
    db = await asyncpg.connect(DB_DSN, user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'))
    async with db.transaction():
        await init_db(db)
        await import_movies(MOVIES_CSV_PATH, db)
        await import_actors(ACTORS_CSV_PATH, db)
        await create_indices(db)
        await calculate_pairs(db)
        await calculate_bacon(db)
    await db.close()
    print('Done')


if __name__ == '__main__':
    asyncio.run(main())

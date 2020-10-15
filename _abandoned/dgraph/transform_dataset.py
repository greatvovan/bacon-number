"""
Transform the dataset into Dgraph Live Loader format.
Data is parsed from CSV files and loaded into an SQLite database
in order to calculate actor-to-actor pairs.
"""

import csv
import sqlite3
from typing import List, Dict, Set, Any, TextIO
from rdflib import Literal


DB_PATH = 'db/movies.db'
MOVIES_CSV_PATH = 'dataset/movies_metadata.csv'
ACTORS_CSV_PATH = 'dataset/credits.csv'
RDF_DATA_PATH = 'actors.rdf'
actors_lookup = {}  # type: Dict[str, int]


def init_db(dbpath: str):
    db = sqlite3.connect(dbpath)
    cur = db.cursor()
    cur.execute('DROP TABLE IF EXISTS movies')
    cur.execute('DROP TABLE IF EXISTS actors')
    cur.execute('DROP TABLE IF EXISTS cast_data')
    cur.execute('CREATE TABLE movies (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)')
    cur.execute('CREATE TABLE actors (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)')
    cur.execute('CREATE TABLE cast_data (movie_id INTEGER NOT NULL, actor_id INTEGER NOT NULL)')
    cur.close()
    return db


def import_movies(fpath: str, db: sqlite3.Connection):
    print('Importing movies from the dataset...')
    add_movie = 'INSERT INTO movies VALUES(?, ?)'
    cur = db.cursor()
    counter = 0
    with open(fpath, 'rt', newline='') as csvfile:
        csvfile.readline()
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            id_ = row[5]
            name = row[8]
            try:
                iid = int(id_)
                cur.execute(add_movie, (iid, name))
                counter += 1
            except sqlite3.IntegrityError:      # WTF?
                print(f'Duplicate movie ID: {id_} ({name}), skipping...')
            except ValueError:                  # Unquoted string
                print(f'Unexpected value for ID: {id_}, skipping...')

    cur.close()
    print(counter, 'movies imported')


def import_actors(fpath: str, db: sqlite3.Connection):
    print('Importing cast data from the dataset...')
    cur = db.cursor()
    counter = 0
    with open(fpath, 'rt', newline='') as csvfile:
        csvfile.readline()
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            cast = eval(row[0])
            movie_id = int(row[2])
            add_cast(movie_id, cast, cur)
            counter += 1
            if counter % 10000 == 0:
                print(counter, 'rows processed')
    print(counter, 'rows processed')
    print(len(actors_lookup), 'actors imported')


def upsert_actor(name: str, cur: sqlite3.Cursor) -> int:
    query = 'INSERT INTO actors(name) VALUES(?)'
    actor_id = actors_lookup.get(name)
    if actor_id is not None:
        return actor_id

    cur.execute(query, (name,))
    actor_id = cur.lastrowid
    actors_lookup[name] = cur.lastrowid
    return actor_id


def add_cast(movie_id: int, cast: List[Dict[str, Any]], cur: sqlite3.Cursor):
    query = 'INSERT INTO cast_data VALUES (?, ?)'
    for actor in cast:
        actor_id = upsert_actor(actor['name'], cur)
        cur.execute(query, (movie_id, actor_id))


def create_indices(db: sqlite3.Connection):
    print('Creating indices...')
    cur = db.cursor()
    cur.execute('CREATE INDEX cast_data_idx ON cast_data(actor_id, movie_id)')
    cur.execute('CREATE INDEX cast_data_idx2 ON cast_data(movie_id, actor_id)')
    cur.close()


def export_to_rdf(db: sqlite3.Connection, fpath: str):
    print('Exporting data to RDF...')
    query = '''
        select distinct a1.id as a1_id, a1.name as a1_name, a2.id as a1_id, a2.name as a2_name
        from actors a1
        join cast_data c1 on a1.id = c1.actor_id
        --join movies m on c1.movie_id = m.id
        --join cast_data c2 on m.id = c2.movie_id
        join cast_data c2 on c1.movie_id = c2.movie_id
        join actors a2 on c2.actor_id = a2.id
        where a1.id < a2.id
    '''
    cur = db.cursor()
    cur.execute(query)
    counter = 0
    created = set()
    with open(fpath, 'wt') as file:
        while True:
            rows = cur.fetchmany()
            if not rows:
                break
            for row in rows:
                create_pair(row, file, created)
                counter += 1
                if counter % 100000 == 0:
                    print(counter, 'pairs processed')
    print(counter, 'pairs processed')
    cur.close()


def create_pair(row: list, f: TextIO, created: Set[int]):
    a1_id, a1_name, a2_id, a2_name = row

    if a1_id not in created:
        rdf_name = Literal(a1_name)
        f.write(f'_:u{a1_id} <Actor.name> {rdf_name.n3()} .\n')
        f.write(f'_:u{a1_id} <dgraph.type> "Actor" .\n')
        created.add(a1_id)

    if a2_id not in created:
        rdf_name = Literal(a2_name)
        f.write(f'_:u{a2_id} <Actor.name> {rdf_name.n3()} .\n')
        f.write(f'_:u{a2_id} <dgraph.type> "Actor" .\n')
        created.add(a2_id)

    f.write(f'_:u{a1_id} <peer> _:u{a2_id} .\n')
    f.write(f'_:u{a2_id} <peer> _:u{a1_id} .\n')


def main():
    db = init_db(DB_PATH)
    import_movies(MOVIES_CSV_PATH, db)
    import_actors(ACTORS_CSV_PATH, db)
    create_indices(db)
    db.commit()
    export_to_rdf(db, RDF_DATA_PATH)
    db.close()


if __name__ == '__main__':
    main()

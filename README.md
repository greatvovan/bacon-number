# Bacon Number API

HTTP API for evaluation of [Bacon Numbers](https://en.wikipedia.org/wiki/Six_Degrees_of_Kevin_Bacon)
of actors.

The service uses Postgres database as the persistent storage and uses disk cache for quick launch.

## Installation

Clone the repository:

```git clone https://github.com/greatvovan/bacon-number```

Launch the services:
```
cd bacon-number
docker-compose up -d --build
```

__Hint: subsequent times you can omit `--build` key to speed up start up.__

Now initialize the database. The service uses Postgres
database as primary storage of actors relationships. Container named
`init` will parse the dataset from CSV files and store it to DB.

```
docker-compose exec init unzip dataset/*.zip -d dataset
docker-compose exec init python dataset_to_db.py
```

You need to do it only one time. When it is done (in around 1.5 minutes),
you can remove `init` container from `docker-compose.yaml` as it is
not needed any more.

HTTP API service (in container named `httpapi`) launches in waiting
state, meaning it will block until the process of data population in
Postgres is completed. As soon as it is completed, the service begins
building a [NetworkX Graph](https://networkx.github.io/documentation/latest/reference/classes/index.html)
of connections between actors, which takes around 3 minutes. The built
graph is then dumped to disk and subsequent launches will take just
seconds.

Ensure that API has started:

```
docker-compose logs -f httpapi
...
...
...
httpapi_1   | [2020-10-15 10:43:27 +0000] [8] [INFO] Waiting for application startup.
httpapi_1   | [2020-10-15 10:43:41 +0000] [8] [INFO] Application startup complete.
```

Run tests:
```
docker-compose exec httpapi pytest
```

Run the benchmark:
```
$ docker-compose exec httpapi python benchmark.py
Got 3000 random actors
3000 Bacon Numbers calculated in 6.8 (439/s)
Got 6000 random actors
3000 random pair distances calculated in 8.3 (361/s)
```

Play with some actors you know:
```
$ curl http://localhost:8080/bn?name=Tom+Hanks
{"dist":1}

curl 'http://localhost:8080/dist?name1=Jennifer+Aniston&name2=Davood+Goodarzi&path=true'
{"dist":8,"path":["Jennifer Aniston","Olivia Munn","Christopher Maleki","Mahmoud Behraznia","Parviz Parastui","Esmail Soltanian","Moharram Zaynalzadeh","Mohsen Makhmalbaf","Davood Goodarzi"]}

$ curl 'http://localhost:8080/dist?name1=Davood+Goodarzi&name2=Grey+Evans'
{"dist":-1}
```

You can shut down everything by
```
docker-compose down
```
`docker-compose` will create volumes named `pgdata` and `graphcache` to
persist the data, so next time you launch `docker-compose up` in the
same directory, all the data (and graph dump) will be in place.

If you want to delete the volume (and the data) say
```
docker volume rm bacon-number_pgdata bacon-number_graphcache
```

## Endpoints

### `/bn`

Return Bacon Number of an actor.
 
**HTTP request**

`GET /bn`

**Query parameters**
- `name`: actor name,
- `path`: optional `true/false` to indicate that you want to see the
connection path, too.

**Response codes**
- `200`: OK, check the response data,
- `404`: actor was not found in the database,
- `500`: unexpected error occured,
- `503`: service is initializing, retry later.

**Response body**

A JSON with fields `dist` (integer) and `path` (array of strings).

### `/dist`

**HTTP request**

`GET /dist`

**Query parameters**
- `name1`: actor name,
- `name2`: actor name,
- `path`:  optional `true/false` to indicate that you want to see the
connection path, too.

**Response codes**
- `200`: OK, check the response data,
- `404`: actor was not found in the database,
- `500`: unexpected error occured,
- `503`: service is initializing, retry later.

**Response body**

A JSON with fields `dist` (integer) and `path` (array of strings).

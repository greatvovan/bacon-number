import logging
import asyncio
from typing import Optional
from fastapi import FastAPI
from fastapi.responses import Response, PlainTextResponse
from .app import Application, Distance, ActorNotFoundError, NotInitializedError
from .backend import Database, ActorsGraph
from .config import *


fapi = FastAPI(title='Bacon Number API', version='0.1')
logger = logging.getLogger(__name__)
app = None  # type: Optional[Application]


@fapi.on_event('startup')
async def startup():
    global app
    app = build_application()
    await app.init()
    logger.info('Service initialized. Ready to accept connections.')


@fapi.get('/')
async def main():
    return PlainTextResponse(
        'GET /bn?name={actor name}&path={true/false} for Bacon number\n'
        'GET /dist?name1={actor name}&name2={actor name}&path={true/false} for arbitrary actors distance\n')


@fapi.get("/bn")
async def bacon_distance(name: str, path: bool = False):
    try:
        distance = await app.get_bacon_dist(name, path)
        return dist_to_dict(distance)
    except ActorNotFoundError as e:
        return Response(status_code=404, content='No actors with name ' + str(e))
    except NotInitializedError as e:
        return Response(status_code=503, content='Service is initializing', headers={'Retry-After': e.args[0]})


@fapi.get("/dist")
async def actor_distance(name1: str, name2: str, path: bool = False):
    try:
        distance = await app.get_actor_dist_by_name(name1, name2, path)
        return dist_to_dict(distance)
    except ActorNotFoundError as e:
        return Response(status_code=404, content="Some of actors aren't found: " + str(e))
    except NotInitializedError as e:
        return Response(status_code=503, content='Service is initializing', headers={'Retry-After': e.args[0]})


@fapi.get("/rebuild-graph")
async def rebuild_graph():
    await asyncio.create_task(app.rebuild_graph())
    return PlainTextResponse('OK')


def dist_to_dict(dist: Distance):
    result = {'dist': dist.length}
    if dist.path is not None:
        result['path'] = dist.path
    return result


@fapi.on_event("shutdown")
async def shutdown():
    logger.info('Shutting down...')
    await app.close()


def build_application():
    # config_logging()
    db = Database(DB_DSN, DB_USER, DB_PASSWORD)
    graph = ActorsGraph()
    return Application(db, graph, GRAPH_CACHE_PATH)


def config_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

import os
import logging
import asyncio
from typing import NamedTuple, List, Optional
from .backend.db import Database
from .backend.graph import ActorsGraph


class Distance(NamedTuple):
    length: int
    path: Optional[List[str]] = None


class ActorNotFoundError(Exception):
    pass


class NotInitializedError(Exception):
    pass


class Application:
    bacon_name = 'Kevin Bacon'  # The key actor to serve as the starting point for distance calculations.
    startup_time = 60           # Typical startup time, to return an estimate if the service is not ready.

    def __init__(self, db: Database, graph: ActorsGraph, graph_cache_path: str):
        self.db = db
        self.graph = graph
        self.graph_cache_path = graph_cache_path
        self.bacon_id = 0
        self.logger = logging.getLogger(type(self).__name__)

    async def init(self):
        self.logger.info('Initializing...')
        await self.db.init()
        await self.wait_for_db()
        self.bacon_id = await self.db.get_actor_id(self.bacon_name)

        # Release control to startup code, to avoid killing the process by Uvicorn after timeout.
        # It will return 503 meanwhile.
        asyncio.create_task(self.create_graph())

    async def create_graph(self):
        if os.path.exists(self.graph_cache_path):
            self.logger.warning(f'Found graph dump {self.graph_cache_path}, loading...')
            self.graph.load_from_disk(self.graph_cache_path)
        else:
            self.logger.warning(f'Graph dump {self.graph_cache_path} was not found, building from DB data...')
            await self.rebuild_graph()

    async def rebuild_graph(self):
        await self.graph.build_from_pairs(self.db.get_actor_pairs())
        if not os.path.exists(self.graph_cache_path):  # Could be created meanwhile by another process
            self.graph.save_to_disk(self.graph_cache_path)

    async def wait_for_db(self):
        while True:
            if await self.db.table_exists('peers'):
                break
            self.logger.warning('DB is not ready yet, waiting...')
            await asyncio.sleep(5)

    async def get_actor_dist_by_id(self, id1: int, id2: int, with_path: bool) -> Distance:
        if not self.graph.ready:
            raise NotInitializedError(self.startup_time)

        path_ids = self.graph.get_path(id1, id2)
        length = len(path_ids) - 1  # Node <--> Node: 2 nodes, 1 step.

        if with_path:
            path_names = await self.db.get_actor_names(path_ids)
            path = [path_names[id_] for id_ in path_ids]
            return Distance(length, path)
        else:
            return Distance(length)

    async def get_bacon_dist(self, actor_name: str, with_path: bool) -> Distance:
        actor_id = await self.db.get_actor_id(actor_name)
        if actor_id is None:
            raise ActorNotFoundError(actor_name)

        return await self.get_actor_dist_by_id(self.bacon_id, actor_id, with_path)

    async def get_actor_dist_by_name(self, name1: str, name2: str, with_path: bool) -> Distance:
        actor_ids = await self.db.get_actor_ids([name1, name2])

        try:
            id1 = actor_ids[name1]
            id2 = actor_ids[name2]
        except KeyError:
            raise ActorNotFoundError([name1, name2])

        return await self.get_actor_dist_by_id(id1, id2, with_path)

    async def close(self):
        # self.graph.save_to_disk(self.graph_cache_path)
        await self.db.close()

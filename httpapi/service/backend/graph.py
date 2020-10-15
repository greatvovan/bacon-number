import logging
import networkx as nx
from networkx.algorithms.shortest_paths.generic import shortest_path
from networkx.readwrite.gpickle import read_gpickle, write_gpickle
from typing import Optional


class ActorsGraph:
    def __init__(self):
        self.graph = None   # type: Optional[nx.Graph]
        self.logger = logging.getLogger(type(self).__name__)
        self.ready = False

    def load_from_disk(self, fpath: str):
        self.logger.warning(f'Loading graph data from {fpath}...')
        self.graph = read_gpickle(fpath)
        self.ready = True
        self.logger.warning('Graph was loaded from disk')

    def save_to_disk(self, fpath: str):
        self.logger.warning(f'Saving graph data to {fpath}...')
        write_gpickle(self.graph, fpath)
        self.logger.warning('Graph was saved to disk')

    async def build_from_pairs(self, pairs):
        added = set()
        counter = 0
        graph = nx.Graph()
        self.logger.warning('Building graph from DB data...')

        async for id1, id2 in pairs:
            if id1 not in added:
                graph.add_node(id1)
                added.add(id1)

            if id2 not in added:
                graph.add_node(id2)
                added.add(id2)

            graph.add_edge(id1, id2)

            counter += 1
            if counter % 100000 == 0:
                self.logger.warning(f'{counter} edges processed')

        self.logger.warning(f'{counter} edges processed')
        self.graph = graph
        self.ready = True

    def get_path(self, src: int, dst: int):
        try:
            result = shortest_path(self.graph, src, dst)
        except (nx.NetworkXNoPath, nx.NodeNotFound):    # Isolated single nodes are not added.
            result = []

        return result

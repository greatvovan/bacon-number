import pytest
import random
from service.app import Application
from service.backend.db import Database
from service.backend.graph import ActorsGraph
from utils import get_random_string
from unittest.mock import Mock, AsyncMock


# noinspection PyUnresolvedReferences
@pytest.mark.asyncio
async def test_get_bacon_dist():
    app = get_application_with_randomized_mock_dependencies()
    actor_name = get_random_string()
    mock_path_ids = app.graph.get_path.return_value
    mock_path_names = list(app.db.get_actor_names.return_value.values())
    mock_actor_id = app.db.get_actor_id.return_value

    dist = await app.get_bacon_dist(actor_name, True)

    assert dist.length == len(mock_path_ids) - 1
    assert dist.path == mock_path_names
    app.db.get_actor_id.assert_awaited_once_with(actor_name)
    app.graph.get_path.assert_called_once_with(app.bacon_id, mock_actor_id)
    app.db.get_actor_names.assert_awaited_once_with(mock_path_ids)


# noinspection PyUnresolvedReferences
@pytest.mark.asyncio
async def test_get_actor_dist_by_name():
    app = get_application_with_randomized_mock_dependencies()
    names_dict = app.db.get_actor_ids.return_value
    name1, name2 = list(names_dict.keys())
    id1, id2 = list(names_dict.values())
    mock_path_ids = app.graph.get_path.return_value
    mock_path_names = list(app.db.get_actor_names.return_value.values())

    dist = await app.get_actor_dist_by_name(name1, name2, True)

    assert dist.length == len(mock_path_ids) - 1
    assert dist.path == mock_path_names
    app.db.get_actor_ids.assert_awaited_once_with([name1, name2])
    app.graph.get_path.assert_called_once_with(id1, id2)
    app.db.get_actor_names.assert_awaited_once_with(mock_path_ids)


def get_application_with_randomized_mock_dependencies():
    length = random.randint(3, 9)
    path_ids = [random.randint(1, 10000) for _ in range(length + 1)]
    path_dict = {i: get_random_string() for i in path_ids}
    pair_ids = [random.randint(1, 10000), random.randint(1, 10000)]
    pair_dict = {get_random_string(): i for i in pair_ids}

    db = Database('', '', '')
    db.get_actor_id = AsyncMock(return_value=random.randint(1, 10000))
    db.get_actor_ids = AsyncMock(return_value=pair_dict)
    db.get_actor_names = AsyncMock(return_value=path_dict)

    graph = ActorsGraph()
    graph.get_path = Mock(return_value=path_ids)
    graph.ready = True

    app = Application(db, graph, '')
    app.bacon_id = random.randint(3, 9)

    return app

import random
from fastapi.testclient import TestClient
from service import api
from utils import get_random_string
from unittest.mock import AsyncMock
from service.app import Distance, ActorNotFoundError


client = TestClient(api.fapi)


class ApplicationMock:
    pass


# noinspection PyUnresolvedReferences
def test_bn_ok():
    api.app = get_randomized_application_mock()
    distance = api.app.get_bacon_dist.return_value
    actor_name = get_random_string()

    response = client.get(f'/bn?name={actor_name}&path=true')

    assert response.status_code == 200
    result = response.json()
    assert result['dist'] == distance.length
    assert result['path'] == distance.path
    api.app.get_bacon_dist.assert_awaited_once_with(actor_name, True)


# noinspection PyUnresolvedReferences
def test_dist_ok():
    api.app = get_randomized_application_mock()
    name1 = get_random_string()
    name2 = get_random_string()
    distance = api.app.get_actor_dist_by_name.return_value

    response = client.get(f'/dist?name1={name1}&name2={name2}&path=true')

    assert response.status_code == 200
    result = response.json()
    assert result['dist'] == distance.length
    assert result['path'] == distance.path
    api.app.get_actor_dist_by_name.assert_awaited_once_with(name1, name2, True)


def test_bn_404():
    app = ApplicationMock()
    app.get_bacon_dist = AsyncMock(side_effect=ActorNotFoundError('XXX'))
    api.app = app

    response = client.get(f'/bn?name=XXX&path=true')

    assert response.status_code == 404
    app.get_bacon_dist.assert_called_once_with('XXX', True)


def test_dist_404():
    app = ApplicationMock()
    app.get_actor_dist_by_name = AsyncMock(side_effect=ActorNotFoundError(['XXX', 'YYY']))
    api.app = app

    response = client.get(f'/dist?name1=XXX&name2=YYY&path=true')

    assert response.status_code == 404
    app.get_actor_dist_by_name.assert_called_once_with('XXX', 'YYY', True)


def get_randomized_application_mock():
    app = ApplicationMock()
    dist = random.randint(3, 9)
    mock_path = [get_random_string() for _ in range(dist + 1)]
    mock_result = Distance(dist, mock_path)
    app.get_bacon_dist = AsyncMock(return_value=mock_result)
    app.get_actor_dist_by_name = AsyncMock(return_value=mock_result)

    return app

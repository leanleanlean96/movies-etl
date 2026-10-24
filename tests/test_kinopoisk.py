import pytest
import requests

from movies_pipeline.exceptions import MaxPagesBudgetExceeded, QueryPageLimitExceeded
from movies_pipeline.sources.kinopoisk import (
    fetch_kinopoisk,
    iter_pages,
    iter_partitioned_pages,
)


def fake_fetch(pages_by_bucket):
    def fetch(endpoint, *, api_key, params, session):
        pages = pages_by_bucket[params["votes.kp"]]
        index = int(params.get("next", 0))
        has_next = index + 1 < len(pages)
        return {
            "docs": pages[index],
            "hasNext": has_next,
            "next": str(index + 1) if has_next else None,
        }

    return fetch


PARTITIONS = [{"votes.kp": "a"}, {"votes.kp": "b"}]


def collect(pages_by_bucket, *, max_pages_total=10, max_pages_per_query=3):
    return list(
        iter_partitioned_pages(
            "/movie",
            {"type": "movie"},
            PARTITIONS,
            max_pages_total=max_pages_total,
            max_pages_per_query=max_pages_per_query,
            session=object(),
            fetch=fake_fetch(pages_by_bucket),
        )
    )


def test_pages_are_labeled_with_their_partition():
    result = collect({"a": [[{"id": 1}], [{"id": 2}]], "b": [[]]})

    assert [(part, page["docs"]) for part, page in result] == [
        ({"votes.kp": "a"}, [{"id": 1}]),
        ({"votes.kp": "a"}, [{"id": 2}]),
        ({"votes.kp": "b"}, []),
    ]


def test_partition_over_api_page_limit_fails():
    with pytest.raises(QueryPageLimitExceeded):
        collect({"a": [[{"id": i}] for i in range(4)], "b": [[]]})


def test_total_page_budget_is_enforced():
    with pytest.raises(MaxPagesBudgetExceeded):
        collect(
            {"a": [[{"id": 1}], [{"id": 2}]], "b": [[{"id": 3}], [{"id": 4}]]},
            max_pages_total=3,
        )


def cursor_fetch(pages, calls=None):
    def fetch(endpoint, *, api_key, params, session):
        if calls is not None:
            calls.append(dict(params))
        index = int(params.get("next", 0))
        has_next = index + 1 < len(pages)
        return {
            "docs": pages[index],
            "hasNext": has_next,
            "next": str(index + 1) if has_next else None,
        }

    return fetch


def test_iter_pages_follows_cursor_until_last_page():
    calls = []
    pages = list(
        iter_pages(
            "/movie",
            {"type": "movie"},
            max_pages=5,
            session=object(),
            fetch=cursor_fetch([[{"id": 1}], [{"id": 2}], [{"id": 3}]], calls),
        )
    )

    assert [page["docs"] for page in pages] == [[{"id": 1}], [{"id": 2}], [{"id": 3}]]
    assert calls == [
        {"type": "movie"},
        {"type": "movie", "next": "1"},
        {"type": "movie", "next": "2"},
    ]


def test_iter_pages_empty_result_is_one_page():
    pages = list(
        iter_pages("/movie", max_pages=5, session=object(), fetch=cursor_fetch([[]]))
    )

    assert pages == [{"docs": [], "hasNext": False, "next": None}]


def test_iter_pages_fails_when_more_pages_than_budget():
    with pytest.raises(MaxPagesBudgetExceeded):
        list(
            iter_pages(
                "/movie",
                max_pages=2,
                session=object(),
                fetch=cursor_fetch([[{"id": i}] for i in range(3)]),
            )
        )


def test_iter_pages_fails_when_has_next_without_cursor():
    def fetch(endpoint, *, api_key, params, session):
        return {"docs": [{"id": 1}], "hasNext": True, "next": None}

    with pytest.raises(RuntimeError, match="no next cursor"):
        list(iter_pages("/movie", max_pages=5, session=object(), fetch=fetch))


@pytest.mark.parametrize("params", [{"next": "x"}, {"prev": "x"}])
def test_iter_pages_rejects_cursor_params(params):
    with pytest.raises(ValueError, match="cursor params"):
        list(iter_pages("/movie", params, max_pages=1, session=object()))


@pytest.mark.parametrize(
    "endpoint", ["movie", "//evil.example/movie", "/movie?limit=1"]
)
def test_fetch_rejects_bad_endpoint(endpoint):
    with pytest.raises(ValueError, match="endpoint"):
        fetch_kinopoisk(endpoint, api_key="key", session=object())


def test_fetch_requires_api_key(monkeypatch):
    monkeypatch.delenv("KINOPOISK_API_KEY", raising=False)

    with pytest.raises(ValueError, match="API key is required"):
        fetch_kinopoisk("/movie", session=object())


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def make_response(status, body):
    response = requests.Response()
    response.status_code = status
    response.reason = "Forbidden" if status == 403 else "OK"
    response._content = body.encode()
    return response


def test_fetch_error_includes_api_message():
    session = FakeSession(make_response(403, '{"message":"Демо: до 10 страниц"}'))

    with pytest.raises(requests.HTTPError, match="403 Forbidden.*до 10 страниц"):
        fetch_kinopoisk("/movie", api_key="key", session=session)


def test_fetch_sends_key_in_header_and_params_in_query():
    session = FakeSession(make_response(200, '{"docs": []}'))

    assert fetch_kinopoisk(
        "/movie", api_key="key", params={"limit": 1}, session=session
    ) == {"docs": []}
    url, kwargs = session.calls[0]
    assert url == "https://api.poiskkino.dev/v1.5/movie"
    assert kwargs["headers"]["X-API-KEY"] == "key"
    assert kwargs["params"] == {"limit": 1}


def test_fetch_rejects_non_object_json():
    session = FakeSession(make_response(200, "[]"))

    with pytest.raises(TypeError, match="JSON object"):
        fetch_kinopoisk("/movie", api_key="key", session=session)

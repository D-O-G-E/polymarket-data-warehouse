from ingestion.gamma import GammaClient


class FakeHttp:
    """Duck-typed HttpClient returning scripted keyset pages."""

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get_json(self, url, params=None):
        self.calls.append(dict(params or {}))
        cursor = (params or {}).get("after_cursor")
        return self.pages[cursor]


def _page(ids, next_cursor):
    return {"markets": [{"id": i} for i in ids], "next_cursor": next_cursor}


def test_follows_cursor_chain_to_terminal_page():
    http = FakeHttp(
        {
            None: _page(["1", "2"], "c1"),
            "c1": _page(["3"], "c2"),
            "c2": _page([], None),  # terminal: empty, no cursor
        }
    )
    got = list(GammaClient(http, "https://x", page_limit=2).iter_markets())
    assert [m["id"] for m in got] == ["1", "2", "3"]
    # first call has no cursor, subsequent calls pass it through
    assert "after_cursor" not in http.calls[0]
    assert http.calls[1]["after_cursor"] == "c1"


def test_stops_on_repeated_cursor_rather_than_looping():
    http = FakeHttp(
        {
            None: _page(["1"], "same"),
            "same": _page(["2"], "same"),  # server keeps returning same cursor
        }
    )
    got = list(GammaClient(http, "https://x").iter_markets())
    assert [m["id"] for m in got] == ["1", "2"]
    assert len(http.calls) == 2


def test_filters_forwarded_and_none_dropped():
    http = FakeHttp({None: _page([], None)})
    list(
        GammaClient(http, "https://x").iter_markets(
            closed="true", volume_num_min=10000, end_date_min=None
        )
    )
    call = http.calls[0]
    assert call["closed"] == "true"
    assert call["volume_num_min"] == 10000
    assert "end_date_min" not in call

from ingestion.gamma import parse_stringified_list, yes_token_id


def test_parses_json_string_field():
    assert parse_stringified_list('["Yes", "No"]') == ["Yes", "No"]


def test_passes_through_real_lists():
    assert parse_stringified_list(["a", "b"]) == ["a", "b"]


def test_tolerates_missing_empty_and_garbage():
    assert parse_stringified_list(None) is None
    assert parse_stringified_list("") is None
    assert parse_stringified_list("not json") is None
    assert parse_stringified_list('{"a": 1}') is None  # JSON but not a list
    assert parse_stringified_list(42) is None


def test_yes_token_id_takes_first_token():
    market = {"clobTokenIds": '["111", "222"]'}
    assert yes_token_id(market) == "111"


def test_yes_token_id_none_when_absent_or_empty():
    assert yes_token_id({}) is None
    assert yes_token_id({"clobTokenIds": "[]"}) is None
    assert yes_token_id({"clobTokenIds": '["", "222"]'}) is None
    assert yes_token_id({"clobTokenIds": "broken"}) is None

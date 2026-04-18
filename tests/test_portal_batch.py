from scrapers.portal import fetch_ohlcv_history_batch


def test_fetch_ohlcv_history_batch_empty():
    out = fetch_ohlcv_history_batch([], sessions=60, max_concurrency=4)
    assert isinstance(out, dict)
    assert out == {}


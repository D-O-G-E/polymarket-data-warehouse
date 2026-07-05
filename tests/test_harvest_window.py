from ingestion.jobs.harvest_prices import compute_window, iter_chunks

DAY = 86_400
NOW = 1_800_000_000


def test_first_fetch_uses_initial_lookback():
    start, end = compute_window(
        NOW, None, initial_lookback_s=30 * DAY, overlap_s=3600, max_window_s=30 * DAY
    )
    assert (start, end) == (NOW - 30 * DAY, NOW)


def test_incremental_fetch_overlaps_watermark():
    wm = NOW - 6 * 3600
    start, end = compute_window(
        NOW, wm, initial_lookback_s=30 * DAY, overlap_s=3600, max_window_s=30 * DAY
    )
    assert start == wm - 3600
    assert end == NOW


def test_stale_watermark_capped_at_max_window():
    wm = NOW - 90 * DAY
    start, _ = compute_window(
        NOW, wm, initial_lookback_s=30 * DAY, overlap_s=3600, max_window_s=30 * DAY
    )
    assert start == NOW - 30 * DAY


def test_chunks_cover_window_exactly_with_no_overlap():
    chunks = list(iter_chunks(NOW - 30 * DAY, NOW, 14 * DAY))
    assert chunks == [
        (NOW - 30 * DAY, NOW - 16 * DAY),
        (NOW - 16 * DAY, NOW - 2 * DAY),
        (NOW - 2 * DAY, NOW),
    ]
    # every chunk within the API's silent-empty limit
    assert all(e - s <= 15 * DAY for s, e in chunks)


def test_small_window_is_single_chunk():
    assert list(iter_chunks(NOW - 3600, NOW, 14 * DAY)) == [(NOW - 3600, NOW)]


def test_empty_window_yields_nothing():
    assert list(iter_chunks(NOW, NOW, 14 * DAY)) == []

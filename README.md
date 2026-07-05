# polymarket-data-warehouse

A production-style ELT pipeline for Polymarket prediction-market data.
This repo is the **ingestion layer**: three Python jobs that pull
Polymarket's public APIs and land raw, append-only JSONL — the bronze
layer that BigQuery and dbt will build on (see [Roadmap](#roadmap)).

No API keys, no wallet, no cost: Polymarket's read endpoints are public.

```
                        ┌─────────────────────────────────────────────┐
                        │                POLYMARKET                   │
                        │  Gamma API              CLOB API            │
                        │  (market/event          (historical         │
                        │   catalog)               prices)            │
                        └─────┬──────────────────────┬────────────────┘
                              │                      │
                  ┌───────────┴─────────┐   ┌────────┴───────────────┐
                  │ sync-catalog        │   │ harvest-prices         │
                  │ (every run)         │   │ (every run, hourly     │
                  │                     │   │  grain, incremental)   │
                  │                     │   │ backfill-prices        │
                  │                     │   │ (one-off, rerunnable)  │
                  └───────────┬─────────┘   └────────┬───────────────┘
                              │                      │
                              ▼                      ▼
              data/raw/raw_markets/          data/raw/raw_price_history/
              data/raw/raw_events/           (one row per token per hour)
              (full payloads, append-only)
                              │                      │
                              └──────────┬───────────┘
                                         ▼
                     [next: load to BigQuery → dbt staging/marts
                      → calibration analysis & dashboard]
```

## Why build a pipeline instead of just querying the API?

Because **the source degrades**. Polymarket prunes fine-grained price
history once a market resolves — verified directly: a resolved 2024
market returns *nothing* at hourly fidelity but 614 points at 12-hour
fidelity. Hourly prices exist only while a market is live. If you want
them after resolution — and every interesting analysis happens after
resolution — someone must have captured them beforehand.

That someone is this pipeline. The harvester captures hourly prices from
live markets on every run; once those markets resolve, this repo's raw
layer holds data the API can no longer give anyone. **The warehouse
becomes more valuable than its source over time.** That is the textbook
reason data platforms exist, in miniature.

The downstream questions this data is collected to answer:

1. **Calibration** (flagship): when Polymarket prices an event at 70%,
   does it happen ~70% of the time? Measured at fixed horizons before
   resolution (24h / 7d / 30d), bucketed by price, with Brier scores and
   confidence intervals.
2. **Favorite–longshot bias**: are 5–15¢ outcomes systematically
   overpriced, as in the horse-racing literature?
3. **Price dynamics**: how early does the market "decide"? Volatility
   and drift as functions of time-to-resolution.
4. **Coherence**: within a multi-outcome event, do the Yes prices sum
   to ~1?
5. **Volume structure**: category growth, concentration, market
   lifecycles.

Questions 2–5 need *zero* ingestion beyond what's already captured —
new questions cost a SQL model, not an engineering sprint. That
flexibility is the point of a warehouse.

## The three jobs

| job | cadence | what it does |
|---|---|---|
| `sync-catalog` | every run (cron) | Lands full raw market + event payloads: everything open, plus anything closed in the last 14 days (captures resolutions). `--full` sweeps the whole catalog once at project start. |
| `harvest-prices` | every run (cron) | The job the warehouse exists for: hourly prices for active markets above a volume floor, incremental via per-token watermarks. |
| `backfill-prices` | manual, rerunnable | Coarse history for already-resolved markets, best-volume first, at whatever fidelity survived pruning. Resumes where it left off. |

```bash
# first-time setup
python -m venv venv
venv\Scripts\activate                       # Windows; source venv/bin/activate elsewhere
pip install -e .[dev]

# first-time data load (order matters)
python -m ingestion sync-catalog --full --volume-floor 10000   # ~45-75 min: catalog baseline
python -m ingestion backfill-prices --max-markets 500          # repeat until it reports already_done for all
python -m ingestion harvest-prices                             # start capturing hourly prices

# recurring (until a scheduler exists, run daily-ish by hand)
python -m ingestion sync-catalog
python -m ingestion harvest-prices

# tests
python -m pytest
```

## Code tour

Reading order if you're new to the codebase — each file is small and
single-purpose:

```
ingestion/
├── config.py          # every tunable, with the reasoning for its default
├── http_client.py     # ONE shared HTTP session: retry w/ backoff, timeout,
│                      #   polite global throttle (4 req/s), User-Agent
├── gamma.py           # catalog client: keyset pagination loop + the
│                      #   tolerant parsers for Gamma's stringified-JSON fields
├── clob.py            # price-history client; documents the two API quirks
│                      #   that shape everything (retention + span cap)
├── sink.py            # the landing zone: append-only JSONL writer,
│                      #   date-partitioned paths, per-row lineage metadata
├── state.py           # watermarks + backfill progress; atomic file writes
├── jobs/
│   ├── sync_catalog.py    # sweep definitions; ~all logic is "which filters"
│   ├── harvest_prices.py  # watermark → window → chunks → land → advance
│   └── backfill_prices.py # fidelity ladder + resume bookkeeping
├── cli.py             # argparse wiring, nothing else
└── __main__.py        # `python -m ingestion` entrypoint

tests/                 # pure-logic tests: parsing, chunking, watermark
                       #   windows, sink layout, state round-trips
```

The separation to notice: **clients** (gamma/clob) know how to talk to
APIs but decide nothing; **jobs** decide what to fetch and when but
contain no HTTP; **sink/state** are the only things that touch disk.
Swapping local JSONL for BigQuery later means replacing sink/state, and
nothing else moves.

## Design decisions (and the reasoning)

1. **ELT, not ETL — land raw, transform later.** Full payloads are
   stored untouched. Any parsing bug or new question is fixable by
   re-running SQL over preserved raw data; nothing is ever
   unrecoverable. Transformation belongs to dbt, downstream.

2. **Append-only raw layer; idempotency lives downstream.** Jobs never
   update or delete. Re-running lands duplicate rows *by design*; dbt
   staging models dedupe on natural keys ((`token_id`, `t`) for prices,
   latest-per-id for catalog). This makes every job trivially safe to
   re-run, which is most of what "production-grade" means.

3. **Watermarks advance only after data lands.** The harvester tracks
   the last landed timestamp per token. A crash mid-run means the next
   run refetches a little — never skips. Fetches also start 1h *before*
   the watermark (duplicates are harmless, gaps are not).

4. **Fetch windows are chunked to ≤14 days.** The API returns silently
   empty for `startTs`/`endTs` spans over 15 days (see API facts below) —
   a failure mode with no error message, caught only by probing. A naive
   30-day first fetch would land nothing and look like success.

5. **Backfill walks a fidelity ladder** (60 → 180 → 360 → 720 → 1440
   min) and tags every row with the fidelity actually obtained, so
   analyses can filter honestly instead of silently mixing granularities.

6. **Only the Yes-side token is fetched.** Binary markets satisfy
   No = 1 − Yes; fetching both doubles API load for zero information.

7. **A declared volume floor ($10k lifetime) scopes all jobs.** The full
   catalog is ~2M markets, ~70% of which never traded $10k (median
   lifetime volume ≈ $2k). The floor cuts sweep cost ~5× while keeping
   everything the analyses use, and it's one config knob to change.

8. **Per-token error isolation, loud aggregate failure.** One bad token
   logs an error and moves on; >10% of tokens failing raises and exits
   nonzero — which is what will turn a scheduled CI run red instead of
   silently landing partial data.

9. **No Spark, no Iceberg, no Kafka.** This data is gigabytes at most.
   A warehouse plus SQL handles it with zero ops burden; distributed
   compute would be resume-driven engineering. (Porting the same
   pipeline to a lakehouse is a deliberate possible v2, as a
   compare-and-contrast exercise.)

## Polymarket API facts (verified empirically, 2026-07)

Everything below was established by probing the live API, not from
documentation — several of these contradict what you'd assume, and two
were discovered only because probes failed in interesting ways.

- Gamma (`gamma-api.polymarket.com`) caps every page at **100 rows**
  regardless of `limit`, and rejects offsets beyond a few thousand. The
  error message points to `/markets/keyset` — cursor pagination via
  `after_cursor`/`next_cursor` (documented for events in the OpenAPI
  spec, undocumented but working for markets). All sweeps use keyset.
- A floored markets sweep (`volume_num_min`) **without an explicit
  `closed` param silently returns only open markets** — ~4.8k rows where
  open+closed is in the hundreds of thousands. Every sweep here passes
  `closed` explicitly. (Events don't have this quirk.)
- `outcomes`, `outcomePrices`, `clobTokenIds` arrive as **JSON-encoded
  strings**, not arrays (`'["Yes", "No"]'`). Old markets ship broken or
  empty values; parsing is tolerant, skips are counted and logged.
- CLOB `/prices-history` takes the **CLOB token id** (a 77-digit
  number, from `clobTokenIds`) — *not* the condition id. Confusing the
  two ids is the classic mistake with this API.
- It accepts either `startTs`/`endTs` or `interval` (`1d`, `1w`, `max`,
  …) — never both — plus `fidelity` (resolution in minutes).
- **Spans over 15 days return silently empty** at any fidelity;
  ~30+ days is a hard 400 (`"interval is too long"`). `interval=max`
  is exempt. Hence chunking (decision 4).
- **Fine-grained history is pruned after resolution**: the same resolved
  market returns 0 points at `fidelity=60` and 614 at `fidelity=720`.
  Hence harvest-live + backfill-coarse (decisions 3–5).
- Market resolution presents as `closed: true`,
  `umaResolutionStatus: "resolved"`, terminal `outcomePrices`
  (`["1", "0"]`), and a `closedTime`.
- Catalog scale: market ids reach ~2.8M with ~70% live → **~2M markets**;
  events ~900k ids, ~75% live. Fetching politely at 4 req/s, a full
  unfloored sweep is ~26,500 pages ≈ 2.5–4 h and ~18 GB of JSONL.

## Data layout

```
data/raw/<table>/dt=<YYYY-MM-DD>/<run_id>.jsonl   # append-only, never mutated
state/ingestion_state.json                        # watermarks + backfill progress
```

Losing `state/` is safe: it causes refetching, never data loss.
The `dt=` partition folders and newline-delimited JSON map 1:1 onto a
`bq load` into date-partitioned BigQuery tables.

| table | grain | row shape |
|---|---|---|
| `raw_markets` | one row per market per fetch | `{_ingested_at, _run_id, _source, payload: <full Gamma market>}` |
| `raw_events` | one row per event per fetch | same, with event payload |
| `raw_price_history` | one row per (token, timestamp) | `{token_id, market_id, condition_id, t, p, fidelity_minutes}` + same metadata |

Every row carries `_ingested_at`, `_run_id`, `_source` — enough lineage
to trace any datapoint back to the exact run that landed it.

## Configuration

Defaults live in `ingestion/config.py` with a comment explaining each
choice. Override via `PDW_*` environment variables (e.g.
`PDW_VOLUME_FLOOR=50000`) or CLI flags (`--volume-floor`, `--data-dir`,
`--rate-delay`, …). `python -m ingestion <job> --help` lists the flags.

## Roadmap

This repo is step 1 of 4. The full shape, in build order:

1. **Ingest** *(this repo — done)*: raw JSONL landing zone. ✅
2. **Load**: ship landed files into a BigQuery (free sandbox) dataset;
   watermark state moves from the local JSON file into the warehouse
   itself (`SELECT MAX(t) … GROUP BY token_id`), which is what makes
   ephemeral CI runners viable.
3. **Transform**: dbt — staging models that parse the stringified JSON
   and dedupe; a snapshot on market metadata (questions and end-dates
   get edited: a real SCD2); marts `dim_markets`, `fct_prices`,
   `fct_resolutions`, `mart_calibration`; schema tests (unique,
   not-null, price ∈ [0,1], relationships) and source-freshness checks
   so a dead harvester fails loudly.
4. **Ops + serve**: Dockerfile; GitHub Actions cron (~6h) running
   ingest → load → `dbt build`; a small dashboard publishing the
   calibration curve and Brier scores by category.

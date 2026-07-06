# This project, explained from scratch

The [README](README.md) describes the system for someone who already
speaks data engineering. This document is the opposite: it assumes you're
new to all of it and spells everything out — the ideas, the vocabulary,
what every file does and why, and an honest answer to "isn't this
overengineered?"

Read it top to bottom once, then keep it open next to the code.

---

## Part 1 — The big ideas

### What a data pipeline actually is

A script that fetches data once is not a pipeline. A **pipeline** is a
system that moves data from where it's born (an API, an app database) to
where it's useful (clean tables you can query), **on a schedule,
unattended, for months** — and survives everything reality throws at it:
the API times out at 3am, returns duplicates, changes behavior silently,
or your laptop dies mid-run.

Almost every design choice in this repo that looks "extra" is an answer
to one of those failures. That's the lens to read the code through:
*"what failure is this line defending against?"*

### What a data warehouse is, and why bother

A **warehouse** is one queryable home for data accumulated over time —
here it'll be BigQuery, with tables like "every hourly price of every
market". Why not just call Polymarket's API whenever you have a question?

Because this particular source **destroys its own data**. Once a market
resolves, Polymarket deletes its fine-grained price history (verified
directly: a resolved market returns nothing at hourly resolution, but still
returns 12-hourly points). Every interesting question — "was the market
right?" — is about *resolved* markets. So hourly prices must be captured
*while markets are live*, by someone, continuously. That someone is this
pipeline, and the warehouse ends up holding data that no longer exists
anywhere else. That single fact justifies the whole project.

### ELT and the "raw layer"

The modern pattern is **ELT**: Extract, Load, Transform — in that order.

- **Extract**: pull data from the source (this repo).
- **Load**: store it **completely untouched** (this repo's `data/raw/`
  folder; later, BigQuery tables).
- **Transform**: clean, join, reshape — *afterwards*, in SQL, with a tool
  called **dbt** (the next phase of this project).

The untouched copy is called the **raw layer** (or "bronze"). The rule:
never edit it, never delete from it, only append. Why so strict? Because
transformations have bugs. If you clean data *before* storing it and your
cleaning is wrong, the original is gone forever. If you store the
original and clean it afterwards in SQL, any mistake is fixable by
re-running the SQL. Raw data is the save file; transformations are
disposable.

A consequence worth internalizing: **the raw layer captures atoms at the
finest grain you can afford** (individual hourly prices, full market
payloads), because you can always aggregate later but never disaggregate.
New questions then cost a SQL query, not a new ingestion system.

### Idempotency: the one property everything serves

A job is **idempotent** if running it twice produces the same end state
as running it once — no duplicated analysis, no corruption. This is
*the* production property, because in real operations jobs get re-run
constantly: crashes, retries, "did that work? run it again".

This repo achieves it with a two-part deal:

1. Ingestion **appends** whatever it fetched, even if some rows were
   fetched before. It never checks "do I already have this?" — that
   would be slow, complex, and fragile.
2. The transform layer (dbt, later) **deduplicates** on natural keys —
   e.g. a price point is uniquely identified by (token, timestamp), so
   duplicates collapse.

Result: any job can be killed and re-run at any moment with zero thought.
This was battle-tested during development — an interrupted backfill, a
silently-truncated catalog sweep, files deleted before loading — and in
every case nothing needed cleanup; re-running fixed it.

---

## Part 2 — The vocabulary

| term | plain meaning | where in this repo |
|---|---|---|
| **source** | where data comes from | Polymarket's Gamma & CLOB APIs |
| **sink** | where data flows *to* — the opposite of a source. Plumbing metaphor: water flows from source to sink | `sink.py` — writes rows to JSONL files |
| **landing zone** | the folder/tables where freshly ingested data first "lands" | `data/raw/` |
| **raw / bronze layer** | the untouched, append-only copy of source data | contents of `data/raw/` |
| **grain** | what one row represents | e.g. `raw_price_history`: one row = one token at one timestamp |
| **natural key** | the field(s) that uniquely identify a real-world record | (`token_id`, `t`) for prices |
| **deduplication** | collapsing duplicate rows using the natural key | deliberately *not* here — dbt's job later |
| **idempotent** | safe to run twice | all three jobs |
| **incremental** | fetch only what's new, not everything every time | `harvest-prices` |
| **watermark** | the bookmark that makes incremental work: "I have data up to timestamp X" per token | `state.py`, used by `harvest_prices.py` |
| **backfill** | one-off fetch of *historical* data (vs. the recurring fetch of *new* data) | `backfill_prices.py` |
| **lookback** | how far into the past a fetch reaches | 30 days on first sight of a token; 14 for recently-closed catalog rows |
| **pagination** | APIs return big lists in pages; you loop to get them all | `gamma.py` |
| **keyset / cursor pagination** | pagination where the API hands you an opaque "cursor" token to get the next page (instead of `offset=200`, which Gamma caps) | `_iter_keyset` in `gamma.py` |
| **rate limiting / throttling** | deliberately slowing your requests so the API doesn't block you | `http_client.py`, 4 requests/sec |
| **backoff** | when a request fails, wait — then wait longer each retry (1s, 2s, 4s…) | `Retry` config in `http_client.py` |
| **fidelity** | Polymarket's word for price-history resolution, in minutes (60 = hourly) | `clob.py`, both price jobs |
| **run** | one execution of one job, with a unique id | `run_id` like `harvest-prices-20260705T182221Z-8f3b75` |
| **lineage** | being able to trace any row back to the run that produced it | `_run_id`, `_ingested_at`, `_source` on every row |
| **state** | the small memory a pipeline keeps between runs | `state/ingestion_state.json` |
| **orchestration** | the thing that runs jobs on a schedule (Airflow, Dagster, or humble cron) | GitHub Actions cron, next phase |
| **staging model** | dbt SQL that cleans/dedupes raw data | next phase |
| **mart** | the final, polished table an analysis actually queries | next phase (`mart_calibration`) |
| **SCD2 / snapshot** | keeping *history* of a record as it changes ("slowly changing dimension") — market questions get edited; every version is kept | enabled by sync-catalog re-landing payloads each run |

---

## Part 3 — The code, file by file

Total: ~700 lines. Each file has one job. Reading order below.

### `ingestion/config.py` — the knobs

A single frozen dataclass holding every tunable number (volume floor,
lookback days, request delay…), each with a comment explaining its
default. Why a dedicated file: numbers scattered through code become
invisible decisions; collected in one place with rationale, they become
*reviewable* decisions. Env-var overrides (`PDW_*`) exist so the same
code runs on a laptop or in CI with different settings — config changes
without code changes.

### `ingestion/http_client.py` — one polite, resilient HTTP door

Every API call in the whole project goes through this one class. It does
three things:

1. **Timeout** (30s) — a hung request fails instead of hanging the job
   forever.
2. **Retry with backoff** — transient failures (rate-limit 429s, server
   5xx errors) get retried automatically: wait 1s, 2s, 4s, 8s. The
   3am-timeout problem, solved in ten lines of config.
3. **Throttle** — at most 4 requests/second, *globally*, because both
   API clients share this one instance. We're an unauthenticated guest;
   polite guests don't get IP-banned.

### `ingestion/gamma.py` — the catalog client

Gamma is Polymarket's catalog API (what markets/events exist, their
metadata). This file knows how to sweep it: the keyset pagination loop
(ask for a page → get 100 rows + a cursor → ask again with the cursor →
repeat until no cursor). It also holds two small parsing helpers,
because Gamma ships some fields as JSON-encoded *strings* — the parser
is deliberately tolerant (returns `None` instead of crashing) because
one malformed 2021 market must not kill a 300k-row sweep.

Note what this file *doesn't* do: it never decides what to fetch, never
writes anything. Clients speak API; jobs decide; sinks write.

### `ingestion/clob.py` — the price-history client

One method: `price_history(token, ...)` → list of `{t, p}` points
(timestamp, price). Its docstring records the two API behaviors that
shaped the whole design — resolved markets lose fine-grained history,
and requests spanning >15 days return silently empty.

### `ingestion/sink.py` — the writer (the "sink")

Where rows become bytes on disk. `JsonlWriter` writes newline-delimited
JSON (**JSONL**: one JSON object per line — the format BigQuery ingests
natively) to a path that encodes table, date, and run:

```
data/raw/raw_price_history/dt=2026-07-05/harvest-prices-20260705T182221Z-8f3b75.jsonl
```

Every run gets its own file (nothing is ever overwritten — that's the
append-only rule made physical), and every row gets three metadata
fields stamped on: `_ingested_at`, `_run_id`, `_source` (lineage). The
`dt=` folder convention mirrors warehouse date-partitioning, so the
future BigQuery load maps 1:1.

### `ingestion/state.py` — the pipeline's memory

A small JSON file holding: per-token **watermarks** ("I have prices up
to timestamp X") and backfill completion marks ("this token is done").
Two design points: saves are **atomic** (write a temp file, then rename —
so a crash mid-save can't leave a half-written corrupt file), and losing
the whole file is *safe* — jobs would just refetch, and dedup absorbs
the duplicates. State is a bookmark, never the data.

### `ingestion/jobs/sync_catalog.py` — job 1: the catalog

Decides which sweeps to run (open markets; recently-closed markets; or
with `--full`, everything) and lands full payloads. There's barely any
logic here — that's the payoff of the layers below. Re-landing the same
markets every run looks wasteful but is the raw material for SCD2
history: market metadata *changes* (questions edited, close dates
moved), and each run's copy is one frame of that film.

### `ingestion/jobs/harvest_prices.py` — job 2: the reason this exists

For each active market above the volume floor:

1. Look up the token's watermark.
2. Compute the fetch window: from (watermark − 1h overlap) to now — or
   30 days back if the token is new. The overlap means consecutive runs
   *deliberately* refetch a little: duplicates are free (dedup), gaps
   are forever.
3. Split the window into ≤14-day chunks (the API silently returns
   nothing for longer spans — the nastiest bug this project surfaced).
4. Fetch each chunk, write the points, and **only then** advance the
   watermark. This ordering is the whole crash-safety story: die at any
   line, and the next run refetches rather than skips.

One bad token logs an error and the loop continues; but if >10% of
tokens fail, the job raises — that's what will turn a scheduled CI run
red instead of silently landing partial data.

### `ingestion/jobs/backfill_prices.py` — job 3: salvage history

For already-resolved markets (highest volume first), try fidelities from
finest to coarsest (60 → 180 → 360 → 720 → 1440 minutes) and land
whatever survived Polymarket's pruning, tagging each row with the
fidelity actually used — so analyses can honestly say "this series is
12-hourly, not hourly". Finished tokens are marked in state, so the job
resumes across runs and you can chip away with `--max-markets 2000`.

### `ingestion/jobs/load_bigquery.py` — raw files into the warehouse

Covered in depth in Part 7; the one-line version: upload each pending
JSONL file into a BigQuery raw table, then move it to `data/loaded/` so
the filesystem itself records what's been shipped.

### `dashboard/build.py` — the marts become a web page

Covered in Part 10: queries the finished marts and writes one static,
self-contained HTML dashboard.

### `ingestion/cli.py` and `__main__.py` — the front door

Argument parsing and logging setup only. `python -m ingestion <job>`
lands here, which calls the matching `jobs/*.run()`. No logic.

### `tests/` — the safety net

Tests cover the *pure logic*: parsing the stringified JSON, chunking
windows, watermark math, file layout, state round-trips, pagination
cursor-following (against a fake HTTP client — no network in tests).
The API-facing code is verified differently: by running the jobs live,
which we've done repeatedly.

---

## Part 4 — A day in the life of one price point

Concrete end-to-end trace. You run `python -m ingestion harvest-prices`:

1. `__main__.py` → `cli.py` parses arguments, sets up logging, builds a
   `Settings` object.
2. `harvest_prices.run()` creates one `HttpClient`, wraps it in a
   `GammaClient` and a `ClobClient`, opens the `StateStore`.
3. It asks Gamma for active markets above $10k volume, sorted
   biggest-first. `gamma.py` pages through keyset cursors; ~4.8k markets
   stream back.
4. For the World Cup market: parse `clobTokenIds`, take the Yes token.
   The state file says its watermark is yesterday 14:00.
5. Window: yesterday 13:00 (overlap) → now. Under 14 days, so one chunk.
6. `clob.py` calls `/prices-history` with that window at `fidelity=60`;
   back come ~30 points like `{"t": 1783275180, "p": 0.0285}`.
7. Each point becomes one JSON line in
   `data/raw/raw_price_history/dt=2026-07-06/harvest-prices-…jsonl`,
   stamped with `_run_id`, `_ingested_at`, `_source`, plus `token_id`,
   `market_id`, `condition_id`, `fidelity_minutes`.
8. Watermark advances to the newest point's timestamp; every 25 tokens,
   state is checkpointed to disk atomically.
9. The summary logs: markets targeted, fetched, skipped, failed, points
   landed. Nonzero exit if failures crossed the threshold.

Later (next phases): a scheduled workflow loads that file into BigQuery;
a dbt staging model dedupes on (`token_id`, `t`) and converts `t` to a
timestamp; `fct_prices` joins it to market metadata; `mart_calibration`
compares prices at fixed horizons against resolved outcomes; a chart
shows whether 70% means 70%.

---

## Part 5 — Honest answer: is this overengineered?

Here's the 30-line version this could have been:

```python
import requests, json
markets = requests.get("https://gamma-api.polymarket.com/markets?closed=false").json()
for m in markets:
    token = json.loads(m["clobTokenIds"])[0]
    prices = requests.get(f"https://clob.polymarket.com/prices-history?market={token}&interval=1w&fidelity=60").json()
    with open("prices.json", "a") as f:
        f.write(json.dumps({"market": m["question"], "prices": prices}) + "\n")
```

And here's what it does wrong, mapped to the component that exists
because of it:

| naive version's failure | component that fixes it |
|---|---|
| Gets only the first 100 markets (silent pagination cap) | `gamma.py` keyset loop |
| Dies on the first network hiccup; a 3am timeout kills the night's run | `http_client.py` retry/backoff/timeout |
| Hammers the API full-speed → eventually IP-banned | throttle in `http_client.py` |
| Refetches the same week every run; gaps if runs are >1w apart | watermarks (`state.py`) |
| A 30-day request would return *empty* and look like success | chunking in `harvest_prices.py` |
| Crash mid-run → half-written state, unclear what you have | land-then-advance ordering; atomic state saves |
| One malformed market kills the whole run | tolerant parsing; per-token error isolation |
| Can't tell which run produced a bad row | `_run_id` lineage stamps |
| Re-running duplicates everything with no plan to fix it | append-only + dedup-downstream contract |
| "It printed nothing for 20 minutes, is it dead?" | progress logging |

So: **for a one-weekend toy, yes, this would be overengineered.** For a
system meant to run unattended every 6 hours for months — which is the
stated goal, and the thing that makes it a *pipeline* rather than a
script — each piece answers a failure that *actually happened during
development*. This project hit the pagination cap, the silent 15-day emptiness,
the silently-truncated sweep, and an interrupted backfill in the first
two days. The design didn't anticipate hypothetical problems; it
absorbed real ones.

That said, a few things genuinely are "production niceties" you could
delete without changing correctness: the env-var config overrides, the
10% failure threshold (could just be "any failure fails"), and the
progress-log cadence. They're small, and they're the difference between
code that works and code that's pleasant to operate — but if any of them
confused you, know that they're seasoning, not structure.

The structure — clients / jobs / sink / state — is the part to
internalize, because it's the same shape at every scale: replace
`sink.py` with BigQuery and `state.py` with a warehouse query and this
is, honestly, how the real thing looks at companies.

---

## Part 6 — Self-test: the ingestion layer

If you can answer these from memory, you own the ingestion half of this
codebase (Parts 7–11 continue with the warehouse, dbt, and ops):

1. Why does the pipeline exist at all — why not query Polymarket's API
   when you need data? *(It prunes hourly history after resolution; we
   capture it live; the raw layer becomes the only copy.)*
2. What happens if `harvest-prices` is killed halfway through?
   *(Nothing bad: watermarks only advance after rows land, so the next
   run refetches the tail. Duplicates are collapsed downstream.)*
3. Why is the raw layer append-only? *(Originals are unrecoverable;
   transformations are re-runnable. Never destroy what you can't
   refetch — especially from a source that deletes its own history.)*
4. Why do fetch windows overlap the watermark by an hour? *(Duplicates
   are free, gaps are forever.)*
5. Why are fetches chunked to 14 days? *(The API silently returns empty
   for spans >15 days — a failure with no error message.)*
6. Why does the backfill tag rows with `fidelity_minutes`? *(Resolved
   markets only retain coarse history; analyses must know which
   granularity they're looking at to be honest.)*
7. Why only the Yes token? *(No = 1 − Yes; fetching both doubles cost
   for zero information.)*
8. Where does deduplication happen, and why there? *(dbt staging,
   downstream — keeping ingestion dumb-and-append-only makes it
   trivially safe to re-run.)*
9. Why a $10k volume floor? *(~70% of 2M markets never traded $10k;
   the floor cuts cost ~5× and thin markets add noise, not signal. It's
   one declared, changeable number.)*
10. Why no Spark/Airflow/Kafka? *(Gigabytes, not terabytes; a warehouse
    plus cron-style scheduling handles it with zero ops burden. Knowing
    when NOT to use the big tools is the senior answer.)*

---

## Part 7 — The warehouse (BigQuery and the loader)

### What BigQuery is, in one paragraph

BigQuery is Google's **analytical database**: you create *datasets*
(folders) containing *tables*, load data in, and query it with SQL. Two
things distinguish it from a normal database like Postgres. First, it's
**columnar** — it stores each column separately, so a query touching two
columns of a 300k-row table reads only those columns, which is exactly
the shape of analytical work ("average price by day") as opposed to app
work ("fetch user #42"). Second, it's **serverless**: there is no machine
to size, patch, or restart, and you pay per gigabyte stored and scanned —
at this project's scale, effectively nothing (the always-free tier covers
10 GiB stored and 1 TiB queried per month).

This project's three datasets map exactly to the architecture layers:

| dataset | layer | contents |
|---|---|---|
| `polymarket_raw` | bronze | `raw_markets`, `raw_price_history` — untouched, append-only |
| `polymarket_dw` | transform | dbt's staging views and mart tables |
| `polymarket_snapshots` | history | the SCD2 snapshot of market metadata |

### The loader (`ingestion/jobs/load_bigquery.py`)

The job is deliberately boring: for each pending JSONL file under
`data/raw/`, run a BigQuery *load job* (a bulk file upload — free, and
separate from query quota), and on success **move the file to
`data/loaded/`**. That move is the entire bookkeeping system: a file's
location tells you whether it's been shipped. No database of load
history, nothing to drift out of sync, and if a crash lands between load
and move, the rerun loads the file again — producing duplicates that
staging dedupes anyway. The same idempotency deal as everywhere else.

Two schema decisions worth understanding:

- **Catalog payloads land in a single JSON-typed column**, not ninety
  autodetected columns. Gamma adds and renames fields freely; a schema
  inferred from today's payloads breaks on next month's. A JSON column
  *cannot* break — new fields just ride along — and dbt extracts the ~20
  fields the models actually use at query time. Price rows, by contrast,
  are flat and stable, so they get real typed columns.
- **Tables are partitioned and clustered.** *Partitioning* physically
  splits a table by a date column (`_ingested_at` for raw, `price_date`
  for `fct_prices`), so a query filtered to one week reads one week's
  bytes, not the whole table. *Clustering* sorts within each partition
  (by `token_id`), so "one token's history" is a short contiguous read.
  These are the two standard BigQuery cost/performance levers, applied
  where the access patterns are known.

### A lesson learned: sandbox vs. billing

The project initially ran in BigQuery's no-credit-card *sandbox*, which
turned out to have two production-fatal limits, both discovered the hard
way. It forbids **DML** (UPDATE/MERGE/DELETE) — which broke the dbt
snapshot the second time it ran, because snapshots MERGE new history
into an existing table. And it silently stamps **60-day expirations** on
every dataset, table, and partition — meaning a warehouse whose entire
purpose is preserving data the source deletes would have started
deleting its own data at day 60. The fix was enabling billing (the
always-free tier still applies; realistic cost is about $0) and
stripping the expirations. The general lesson: free tiers fail loudly on
features and *silently* on retention — read the retention fine print
first.

---

## Part 8 — dbt (the transform layer)

### What dbt is and why it's everywhere

dbt ("data build tool") turns a pile of SQL into something with software
engineering properties. Each *model* is just a SELECT statement in a
file; dbt materializes it as a table or view in the warehouse. The value
is in what surrounds that:

- Models reference each other with `{{ ref('stg_prices') }}` instead of
  hard-coded table names, so dbt knows the whole **dependency graph
  (DAG)** and builds everything in the right order.
- **Tests** are declared next to the models in YAML ("this column is
  unique", "this value is between 0 and 1") and run as real queries.
- Everything is **version-controlled text** — a schema change is a code
  review, not a mystery someone ran in a console.

This layer is where analytics teams live all day, which is why dbt
fluency is such a strong hiring signal.

### The models here, and what each one teaches

**Staging** (`stg_markets`, `stg_prices` — materialized as *views*,
meaning they store nothing and re-compute when queried): this is where
raw's mess becomes clean. `stg_markets` parses the JSON payload —
including the double-parse of Gamma's JSON-inside-a-string fields — and
keeps only the latest fetch per market (`ROW_NUMBER() OVER (PARTITION BY
market_id ORDER BY _ingested_at DESC) = 1`, the standard dedupe idiom).
`stg_prices` collapses ingestion's deliberate duplicates on
`(token_id, t)`, preferring the finest fidelity, and **clamps prices to
[0, 1]** — a policy that exists because the accepted-range test caught
two real rows at 1.0025 on the first live build: order-book midpoints
can drift marginally past $1 on degenerate books. Raw keeps the original
values untouched; staging applies the documented judgment call.

**Marts** — the tables analyses actually query, in dimensional-modeling
vocabulary: *dimensions* describe things (`dim_markets` — one row per
market), *facts* measure things (`fct_prices` — one row per token per
timestamp). Two worth reading closely:

- `fct_prices` is **incremental**: instead of rebuilding from scratch,
  each run MERGEs only rows ingested since the last build, keyed on
  `price_id` so reprocessing can never duplicate. The
  `{% if is_incremental() %}` block in its SQL is the pattern to
  internalize — it's dbt's single most-asked-about feature.
- `fct_resolutions` derives each market's outcome from its terminal
  prices (`["1","0"]` = Yes won), because "who won" is an *inference*
  from the data, not a field — and the model's comments document exactly
  what's included, excluded (ties, refunds), and cross-checked.

`mart_calibration` is the payoff: for every resolved market, an **as-of
join** picks the last known price at fixed moments before resolution
(1 day / 1 week / 30 days), buckets prices into deciles, and compares
each bucket's average price with how often those markets actually
resolved yes — with Wilson confidence intervals (better behaved than the
normal approximation in small or near-0/1 buckets) and Brier scores
(mean squared error of price vs. outcome; 0.25 is the score of always
guessing 50 cents). The fixed horizons are the methodological heart:
prices five minutes before resolution are trivially "correct", so naive
calibration plots flatter the market.

**The snapshot** (`snapshots/markets_snapshot.sql`) is dbt's **SCD2**
("slowly changing dimension, type 2") feature: market metadata *changes*
— questions get edited, end dates move, volume accumulates — and the
snapshot keeps every version with validity intervals (`dbt_valid_from` /
`dbt_valid_to`). One elegant consequence: daily traded volume doesn't
exist anywhere in the API, but differencing successive snapshot versions
of cumulative volume *derives* it — a fact conjured from a dimension's
history.

**Tests and freshness** close the loop. Schema tests are executable
assumptions (uniqueness, non-null, ranges, relationships); when one
fails, the build goes red *before* a wrong number reaches a chart.
`dbt source freshness` checks how stale the raw tables are — that's the
alarm that fires if the harvester silently stops.

---

## Part 9 — Ops (GitHub Actions, the service account, Docker)

### The scheduler

GitHub Actions rents out throwaway Linux machines that run YAML-defined
steps on a trigger — a cron schedule, a push, or a button. The
`pipeline.yml` workflow is the entire production operation: twice a day,
a fresh machine checks out the repo, installs the package, writes
credentials, then runs exactly the commands a human would: sync-catalog
→ harvest-prices → load-bigquery → `dbt build` → `dbt source freshness`
→ rebuild the dashboard. If any step fails, the run is red and GitHub
emails the repo owner. The machine is destroyed afterward. That is the
whole meaning of "production" here: *unattended, on a schedule, loud on
failure*. (At larger scale this graduates to Airflow or Dagster — worth
it when there are dozens of interdependent pipelines, not three jobs.)

### The ephemerality problem and its solution

Those runner machines keep no disk between runs — so the local state
file, the harvester's memory of what it already fetched, can't live
there. The fix (`--watermarks-from bigquery`) is to derive watermarks
from the warehouse itself: `SELECT token_id, MAX(t) … GROUP BY token_id`
*is* the watermark, reconstructed from what actually landed. This is
better than a file, not just equivalent: if a run harvests but dies
before loading, the warehouse watermark stays behind and the next run
automatically refetches the gap. State that's derived can't drift from
reality, because it *is* reality. The local state file remains for
laptop runs, where it avoids a warehouse round-trip.

### The machine identity

The pipeline authenticates as a **service account** — a machine user
(`pipeline-ci@…`) with exactly two permissions: edit BigQuery data and
run BigQuery jobs. It can't create infrastructure, touch billing, or
read anything else — so if its key ever leaked, the blast radius is one
project's datasets. The key lives in the repo's **encrypted secrets**,
injected into the workflow at runtime, never in code. (War story: the
first key upload was silently corrupted by a Windows shell prepending an
invisible byte-order mark to the JSON — the first cloud run failed with
"not a valid json file" on a file that looked perfectly valid.
Credentials are bytes, not text; treat them accordingly.)

### Docker's role

The `Dockerfile` packages the jobs and the dbt project into one image,
so the pipeline runs identically on any machine — a laptop, CI, a
server. In this setup it's the *portability guarantee* rather than the
runtime: CI installs with pip for speed but builds the image on every
push to prove it stays runnable. The second workflow, `ci.yml`, is the
quality gate: unit tests, an offline `dbt parse` (catches broken
SQL/config without needing a warehouse), and that Docker build, on every
push and PR.

---

## Part 10 — The dashboard

`dashboard/build.py` closes the loop from raw API bytes to something a
human looks at. The design choice to understand: it generates a
**static page** — one self-contained HTML file with inline SVG charts,
no server, no JavaScript framework, no external assets. An app server
(Streamlit and friends) would need a machine awake around the clock to
serve a handful of visits a day; a static file regenerated twice daily
by the pipeline and pushed to the `gh-pages` branch needs nothing and
can't go down. The charts themselves encode the analysis: dots on the
diagonal mean the market was right; whiskers show the uncertainty the
sample size justifies; the bar strips reveal where the data actually
lives (heavily concentrated in the extreme buckets — most markets spend
most of their lives priced near 0 or 1).

---

## Part 11 — Self-test: warehouse, dbt, and ops

1. Why does the raw markets table use one JSON column instead of real
   columns? *(The source renames fields freely; autodetected schemas
   break on drift, a JSON column can't. Staging extracts what's needed
   at query time.)*
2. What do partitioning and clustering buy? *(Queries filtered by date
   read only matching partitions; within a partition, clustering makes
   one token's rows a contiguous read. Cost and speed, derived from
   known access patterns.)*
3. How does the loader know which files it already loaded? *(It doesn't
   keep records — loaded files move to `data/loaded/`. The filesystem is
   the state; a crash between load and move causes only a harmless
   duplicate load.)*
4. View vs. table vs. incremental — which model is which, and why?
   *(Staging = views: always current, store nothing. Marts = tables:
   computed once per build. `fct_prices` = incremental: MERGE only new
   rows, keyed so reruns can't duplicate.)*
5. What does the SCD2 snapshot enable that `dim_markets` can't?
   *(History — e.g., daily volume derived by differencing successive
   versions of cumulative volume; audits of edited questions and moved
   deadlines.)*
6. Why do CI runs derive watermarks from the warehouse? *(Runners keep
   no disk; and derived state self-corrects — a harvest-then-load-crash
   leaves the watermark behind, so the gap is refetched.)*
7. Why a service account instead of a personal login? *(Least privilege
   and blast radius: two roles on one project, revocable, and no human's
   credentials embedded in a robot.)*
8. What happens when a dbt test fails in the scheduled run? *(The build
   exits nonzero, the workflow goes red, GitHub emails the owner — bad
   data is stopped before it reaches the marts' consumers.)*
9. Name a real bug each safety net caught. *(accepted_range: prices of
   1.0025 from degenerate order books. Source freshness: guards the
   silent-dead-harvester case. The snapshot's MERGE: surfaced the
   sandbox DML ban before it could corrupt anything.)*
10. Why is the dashboard a static file? *(Nothing to host, nothing to
    crash; the pipeline regenerates it on schedule. A server earns its
    keep only when readers need live queries, not twice-daily numbers.)*

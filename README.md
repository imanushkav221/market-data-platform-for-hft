# From exchange to quant: a market data platform

Taking commodity and crypto market data from where it is published to where a
researcher can query it, with the correctness and latency guarantees a trading
desk needs, and a forecasting model on top to prove the platform works end to end.

Python, QuestDB, Dagster, Bash on Linux. Runs on a laptop with no network and no
server, on DuckDB.

The store is QuestDB because of a measurement rather than a preference:
[`bench/RESULTS.md`](bench/RESULTS.md) times the as-of join — the query this
platform exists to make correct — on 20M trades, and QuestDB answers it in
0.029 s where ClickHouse takes 1.06 s, with a cost that grows with the question
instead of with the table. ClickHouse was measured and rejected; the benchmark and
the case for reopening that decision are both still in the repo. DuckDB is the
second backend, so the demo and the tests need nothing installed.

```bash
uv sync --frozen                 # pyproject.toml is the source of truth, uv.lock pins it
uv run mdp init
uv run mdp ingest --synthetic
uv run mdp reference
uv run mdp bars --symbol BTCUSDT --freq 5m
uv run mdp continuous --symbol CRUDEOIL --as-of 2026-06-30
uv run mdp model --symbol CRUDEOIL
uv run mdp status
```

Without uv: `pip install -r requirements.txt` then `python -m mdp.cli ...` with
`PYTHONPATH=src`. `requirements.txt` is a generated export of `pyproject.toml`
(`make lock` regenerates it), kept so that path still works; it is not a second
dependency list to edit. `make demo` and the other targets below set
`PYTHONPATH` themselves, so they work either way.

`make demo` runs all of that in order. Orchestrated instead of by hand — Dagster,
and only Dagster, because the unit of work here is a dataset and `@asset` declares
one. The repository used to carry an Airflow DAG and a cron/`flock` script over the
same functions; both are gone, and what they were there to prove is now proved by a
test that parses every module under `src/mdp` for orchestrator imports. That is
checkable and cannot quietly diverge, which a second runner can and did
(`docs/ARCHITECTURE.md` 3.4):

```bash
make dagster-install      # Dagster into its own virtualenv, not the project one
make dagster              # the code location in the UI, with the daemon
make dagster-materialise  # one trading day end to end, no UI
make dagster-backfill     # replay a range: the holidays are not partitions, so nothing skips them
make dagster-arrival      # poll from the publication deadline and load on arrival
make dagster-ticks        # one incremental pass over the tick feed, from the cursor
make dagster-monitor      # the checks that have something to say when nothing ran
make watch                # event-driven: ingest a file the moment it lands
make drop                 # (in another terminal) drop a vendor file in and watch it load
make fetch                # go and get today's file, then load it: the whole loop
make backfill             # replay a date range, weekends and holidays skipped
make monitor              # what should be here and is not
make calendar             # which days the exchange is open, and when the file is due
make asof                 # as-of price and trailing volatility per decision time
make await                # wait for the end-of-day file inside its publication window
make poll                 # keep a continuous source current, taking only what is new
make restore              # rebuild the database from the raw files
make recipe               # run the model recipe: every scenario isolated, then compared
make charts               # figures for the deck, drawn from the run that just happened
make notebook             # execute the research notebook and render it to HTML
make lab                  # the same notebook, interactively
make exporter             # serve /metrics where the pipeline runs
make monitoring           # Prometheus, Alertmanager and Grafana
make monitoring-local     # the same three, as local binaries, on a box with no Docker
make alert-sink           # stands in for Slack or PagerDuty
make metrics              # numbers a scraper can graph, printed once
make retention            # age raw files out of the landing zone (dry run)
make check                # what CI runs: ruff, mypy, pytest
```

### How often it fetches

| Source | Cadence | How it knows what to ask for |
| --- | --- | --- |
| `mcx_bhavcopy` | polls from 23:55 IST until the file lands, or 2 hours pass | One file per trading day, published after the close, so there is nothing to fetch until the session ends. But publication time varies, so it waits inside a window rather than taking one shot |
| `binance_trades` | every 60 seconds | A cursor. Each pass asks for everything since the last **successful** load, with a five second overlap |

The window matters because a single scheduled fetch cannot tell **late** from
**never**, and those need opposite responses. Polling from the deadline gives
three recorded outcomes instead of one guess: on time (nothing to say), late (it
loads, the delay is logged, a warning goes out, and a month of them is a vendor
conversation with evidence), or missing (the window closes and somebody is paged).

Both cadences live in `config/sources/*.yml`, not in code. A continuous venue is
polled; an end-of-day file is waited for; and either can also arrive by being
dropped in the landing directory, which is the same path.

**[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) is the reasoning**: every tool
choice, the alternatives rejected, what was deliberately left out and why, how each
piece maps to the role, and what would change at a terabyte a day.

---

## What it does

```
        acquire            validate           land              load            serve
  exchange / vendor  ->  contract gate  ->  Parquet raw  ->   QuestDB    ->  MarketData client
   (or synthetic)        quarantine         (source of       SAMPLE BY      get_bars()
                         + reasons           truth)           reference      get_continuous()
                             |                                   |
                             +--------> quality checks ----------+
                                        vendor scores
                                        run registry
```

## One platform, two shapes

A trading desk has both an end-of-day file and a live feed, and they fail in
completely different ways. So this is one platform carrying two data shapes, and
each one goes the whole distance: arrival, validation, storage, reference,
consumer. What differs is the mechanics at every stage, not how far it gets.

| Stage | End-of-day crude (medium frequency) | Tick feed (high frequency) |
|---|---|---|
| Arrival | one file a day, waited for inside a publication window | continuous, polled every 60s on a cursor |
| Validation | cross-field rules, coverage against the trading calendar | duplicates on replay, out-of-order ids, clock skew |
| Storage | one row per contract-day, yearly partitions, a correction kept beside the row it corrects | daily partitions, batched inserts, the grain deduplicated at commit, bars aggregated from the trades column on read |
| Reference | expiries, roll map, lot size, all point-in-time | none needed |
| Consumer | continuous back-adjusted series → forecasting model | bars and an as-of join → research-ready rows |
| Silent failure it guards against | a roll that reverses, a stale lot size, a file that never came | a leaked future print, a stale price dressed as current |

Same contract gate, same landing zone, same checks table, same client library.
The tick feed is a public venue because **MCX tick data is licensed and cannot be
obtained for a demo**; the design is identical either way, and saying which half
is a stand-in is better than pretending otherwise.

The commodity is **crude oil**: the most active MCX contract, monthly expiries
around the 19th of the month before delivery, and roughly twice the daily
volatility of bullion, which makes the roll and the model both non-trivial.

## Why it is built this way

**Retrieval is automated and the loader does not know about it.** The fetcher
pulls from the exchange and lands the file in the drop zone; the watcher takes it
from there. The loader cannot tell whether a file arrived because we fetched it,
because a vendor pushed it over SFTP, or because somebody dropped it in by hand
during an incident. One path, three ways in, so replacing the fetcher touches
nothing downstream.

**Nothing is expected on a day the market was shut.** The trading calendar decides
which days a delivery is due and when it is late. Alerting on a Saturday is how
people learn to ignore alerts.

**The check nothing else does: the file that never came.** Every check in
`quality.py` looks at data that arrived. The arrival monitor asks the opposite
question, against the calendar, and pages when a trading day is past its deadline
with nothing loaded. That is the failure that actually costs money, because
nothing breaks loudly. It asks per symbol, not per day: the source declares five
MCX symbols, and a day-level row count means three trading days of entirely
absent CRUDEOIL look like a clean week as long as GOLD arrived. A day with
nothing and a day missing some symbols get different alertnames, because they are
different incidents and the second is the more dangerous — the dataset looks
populated and every query over it silently returns a subset.

**Data is loaded when it arrives, not when a clock says so.** A scheduled
pipeline pays for its schedule: a file landing at 06:02 waits until 07:00, and the
desk has stale data for 58 minutes that nobody can see. The watcher ingests on
arrival and records the gap between the file's own timestamp and the moment its
rows were queryable, so latency is a number in a table rather than a claim. It
handles the three things that actually bite: partial writes (wait for the file to
stop growing), re-delivery (identity is the content hash, so a rename is not a new
file), and a delivery that cannot be read (logged, retried up to a budget, and
never allowed to stop the other sources).

**A cursor, not a window.** Asking for a fixed window is wrong in both
directions: too small loses data when a run is late, too large re-reads forever.
Each source records where it was read up to, the mark moves only after a
successful publish, and it moves to what actually loaded rather than to what was
requested. A blocked load leaves the position alone so the next run picks up the
same data. It is also capped at the end of the window that was asked for, which
is the rule that was missing: one print with a clock two hours fast used to drag
the high-water mark with it, so the next window started after it ended and the
two hours in between were never requested by any run. Not an error, not a retry
— just missing.

**The landing zone is the source of truth, not the database.** Everything that
arrives is written to Parquet before it is interpreted. If a transform turns out
to be wrong, we replay from disk instead of going back to the vendor.

**Nothing is ever half-loaded.** Every dataset has a contract
(`config/contracts/*.json`) declaring columns, types, ranges, keys and
cross-field rules. Rows that fail are quarantined with a reason; a *missing
column* stops the load entirely, because that is a provider changing their format,
not a bad row. A contract can also declare how much of a file may be thrown away
— `max_null_fraction` for rows lost to an absent or uncoercible required value,
`max_reject_fraction` for rows lost to anything at all — and past either limit the
load blocks. Quarantining a handful of bad rows and carrying on is still right;
publishing 49 rows of a 1000-row file because the other 951 were filed as
warnings is not.

**Adding a source is configuration, not code.** A new feed is one YAML file in
`config/sources/` plus one contract. No Python changes. That is the honest answer
to "how quickly can you onboard a new dataset".

**Acquisition and storage are separate.** Retries, paging and provider quirks live
in `acquire.py`; file layout, batching and compression live in `storage.py`.
Either can be replaced without touching the other, which is what lets the same
pipeline run on QuestDB in production and on DuckDB on a laptop with nothing
above the store knowing which.

**Bars are computed from the deduplicated trades, and the backend decides how.**
On QuestDB there is no bars table: `SAMPLE BY` aggregates the time-ordered trades
column at query time, and that is safe because `DEDUP UPSERT KEYS` collapses a
replayed print at commit, so no read can see an uncollapsed duplicate. What is
given up is a precomputed answer — a minute bar over a wide range costs a scan of
the trades in that range — and on this platform's read patterns that is the
better trade. On ClickHouse the same table has to be maintained, and the
interesting part — the reason this is a property of the store and not a style
choice — is that the textbook version is wrong there: an insert-triggered
materialised view fires on the inserted block before `ReplacingMergeTree` has
collapsed anything, so a replayed trade is counted twice in `volume`, `trades` and
`notional` forever and no merge repairs it. The view has to be *refreshable*, over
`trades FINAL`, and correct-and-a-minute-behind beats fresh-and-wrong for a number
a desk trades on. QuestDB needs neither half of that argument, which is why there
is no bars table here at all.

**Reference data is point in time, and the table can actually answer the
question.** The front-month map carries `trade_date` — the day the choice applies
to — alongside `known_from` and `known_to`, the window during which we believed
it. Refreshing it closes a superseded row rather than deleting it, and
`get_continuous(..., as_of=...)` reads the stored map back. That is different
from truncating the prices and re-running the rule, which is what it used to do:
re-running uses today's settlements, so a correction that arrived last week
silently changes which contract was front on a day already inside a published
backtest. A backtest run in June must see June's map, or "why did this number
change?" has no answer.

**Every run leaves a trail.** Quality events, vendor scores and model runs are
tables, not log lines.

## Design decisions worth defending

| Decision | Why | What I rejected |
|---|---|---|
| QuestDB as the primary store | Measured, not assumed. On the as-of join over 20M trades: 0.029 s at 5,000 decision timestamps and 0.081 s at 50,000, +297 MB. 2.8x the time for 10x the work, because the cost is a binary search per decision into a physically time-ordered column store. `bench/RESULTS.md` has the method and the limits | ClickHouse as primary: 1.06 s at both sizes and +839 MB, because the cost is a hash build over the whole right-hand side and as-of semantics fix which side that is. One cost grows with the question, the other with the table |
| ClickHouse measured, then removed rather than half-maintained | The benchmark decided the read path, and after it the second implementation was carrying cost without earning any: its CI job was marked `continue-on-error`, which is a repository admitting it does not maintain a path. An interface over three stores can also only express what all three share, which is the opposite of what you want once a store has been chosen for its specific strengths. The measurement is untouched — `bench/` drives ClickHouse with raw SQL over HTTP, never through the deleted backend | Keeping it as a maintained second backend. What would bring it back is written down rather than implemented: it beats QuestDB on point-in-time reads (`argMax(..., ingested_at) GROUP BY grain` versus `LATEST ON` with several partition keys, which scans), and its open-source build has replication, TLS and role-based access control, all Enterprise-only on QuestDB. For licensed exchange data that is a real cost that appears in none of the numbers |
| `DEDUP UPSERT KEYS` on the QuestDB trades table | Enforced at commit rather than eventually, so there is no window in which a replayed print sits in the table waiting for a merge and no read has to say `FINAL`. Faster and more correct at once | Nothing to reject: the cost is that QuestDB requires the designated timestamp in the key set, so a *corrected* trade timestamp lands beside the original instead of replacing it. Named in `sql/02_questdb.sql` as a runbook entry |
| Batched inserts, sized per source | A column store pays a fixed cost per write that does not shrink with the row count: QuestDB commits a WAL transaction and then waits for the apply job, and ClickHouse creates a part per insert and merges it later. `batch_rows` is per source in config because the tick path and a once-a-day file want different sizes | Row-at-a-time inserts, and a single global batch size |
| Parquet landing zone | Cheap, columnar, replayable, and the raw record if a vendor disputes something. Also the only artefact that cannot be recomputed: a wrong transform is a replay from disk, a lost file is a week of vendor emails | Loading straight to the database: no way back. Keeping the vendor's CSV: honest, 5 to 10 times the storage, slow to replay |
| Dagster as the orchestrator, and only Dagster | The unit here is a dataset, and `@asset` declares one. The platform's contracts become asset checks with no reimplementation, and trading-day partitions are one keyword argument | Prefect, which ranked last on this workload: no partitions and no checks. Airflow, a close second whose data-interval and catchup model is genuinely better for this shape of source — what decided it is that the unit here is a dataset and there is no incumbent scheduler to inherit. A second runner: an Airflow DAG and a cron script used to sit beside the code location, and a static test makes the portability claim better than either did. All argued in full in the architecture document |
| Roll never reverses | A naive "most volume today" rule flips contracts around sixty times a year on this series, because daily volume flips around. Forcing rolls forward gives about twelve, which is what a monthly contract can have | Letting daily volume decide freely: a price series that jumps between contracts |
| Ridge by normal equations | Nine readable lines make the no-lookahead property auditable, and that property is the whole claim. `sklearn.fit()` would do the same thing correctly and hide it | A gradient-boosted model: better numbers, no more insight, and much harder to explain honestly. Where nobody is auditing the fit, sklearn |

## Medium frequency versus high frequency

The same platform, two different sets of constraints.

| | Medium frequency (minutes to EOD) | High frequency (ticks) |
|---|---|---|
| Ingestion | Scheduled or event-triggered batch | Always-on capture process |
| Language in the hot path | Python is fine | Nothing interpreted; capture writes raw, decoding happens after |
| Delivery | Load then validate then publish | Capture first, validate on the way to the served table |
| Back pressure | Not an issue | The whole design: what happens when the consumer falls behind |
| Time | Arrival time is usually good enough | Exchange timestamps, and a clock you trust |
| Replay | From the landing zone | From the raw capture, which is why capture is separate from parsing |
| Storage | Yearly partitions, modest batches | Daily partitions, large batches, aggressive TTL to cold storage |

What does not change: the raw capture is the source of truth, the contract is
enforced before anything is published, and reference data is point in time.

## The as-of join

What a quant asks for more than anything else: given a list of moments — signal
timestamps, order times, model decision points — what was true at each one?

```bash
make asof
```

It sounds like a lookup and it is the easiest place in the whole platform to leak
the future. Two mistakes are nearly universal, and both are guarded here:

- **Joining on the nearest timestamp** rather than the last at or before. Nearest
  will return a print from 200ms *after* the decision, and a backtest built on
  that looks wonderful and is worthless. There is a test that inserts exactly such
  a print and asserts the answer does not move.
- **Silently returning a stale price.** If the last trade was forty minutes ago,
  the honest answer is "no usable price", not a forty-minute-old number presented
  as current. Past the staleness bound the row survives and the price comes back
  empty, so a consumer has to handle the hole instead of trusting a plausible
  wrong number.

The match runs as an as-of join in the database rather than in pandas, because
the database can answer it with a binary search into a time-ordered column and
pandas has to hold both sides in memory. Three things about the QuestDB
implementation were found by running it against a live server and are worth
knowing before reading `storage.py`:

- **A join key that matches nothing does not fail fast, it hangs.** On the 20M-row
  benchmark table the same 5,000 decisions answered in 0.062 s for a symbol that
  exists and hit the server's 60-second timeout for one that does not, because the
  backward scan for a key that will never appear does not stop at a partition
  boundary. `as_of` therefore probes the key first — 2 ms when it matches, about
  100 ms when it does not — and answers a key with no trades without the join. The
  answer is identical: every match would have been null.
- **The decision timestamps are staged in a table rather than inlined.** QuestDB's
  `/exec` is GET-only, so SQL travels in the URL and dies past roughly 64 KB as a
  dropped socket with no error — measured at around 480 decisions for this query.
  Staging costs 36 ms for three timestamps and 76 ms for 5,000, and buys one code
  path whose semantics do not change with N.
- **There is no staleness bound inside the join, on purpose.** 8.2.1 has no
  `TOLERANCE` clause, and it would be the wrong tool anyway: it nulls the match,
  which throws away the matched timestamp — and reporting *how* stale the price we
  refused to return was is the point of refusing it.

Trailing realised volatility
comes from the minute bars, and admits only bars that *end* at or before the
decision time. That is a stricter test than it looks: a bar is stamped with the
start of the interval it covers, so a decision at 10:50:17 taking the bar stamped
10:50:00 is taking a close built from trades after the decision. On this repo's
own demo data that one bar moved realised volatility by a factor of 52. It had
also hidden from the test, which used a decision time falling exactly on a bar
boundary — the single case where the right and wrong versions agree.

## The model layer

A model here is a **recipe**: inputs, features, target, validation and outputs
declared in `config/models/*.yml` with a version on them. The code reads the
recipe; it does not contain the model. Adding a variant is a block of YAML.

**Scenarios are isolated.** Each variation gets its own run id, parameters and
artifacts, and nothing overwrites anything, which is what lets you ask "is the
longer horizon better?" without destroying the run that answered the last
question. It is the same mechanism you would use to shadow a new model version
against the live one before promoting it.

**A run records the data it saw**, not only its parameters: rows, date range and
a hash of the series. Parameters alone do not reproduce a run, because the data
moves underneath them.

```bash
make recipe
```

On the synthetic crude series, six scenarios, the interesting result is that
**more features is the worst of them** — the eleven-feature variant comes last by
roughly an order of magnitude against the three-feature control, and it is also
the variant with the fewest folds, because a longer feature set needs a longer
warm-up. More parameters chasing less data. That is the overfitting lesson
falling out of the harness rather than being asserted, and it is the slide worth
spending a minute on. A single train/test split would very likely have made that
scenario look like the winner.

No figures are quoted here on purpose. The demo series is generated relative to
the day it is run, so any number written into this file would be stale by the
time somebody read it; `make recipe` prints the table, and the ordering above is
what holds across runs.

Not an alpha claim. Three things it demonstrates:

1. **No look-ahead.** Features at row *t* use only information available at the
   close of *t*. The one forward-looking line in `model.py` is the label. There is
   a test that rewrites the future and asserts the past does not move. The
   walk-forward adds a purge gap sized to the horizon, so scenarios predicting one
   day and five days ahead are comparable instead of the longer one quietly
   leaking five times as far into its test block.
2. **Walk-forward against a baseline.** Expanding window, tested on the next block,
   compared against "no change". A model that cannot beat that has told you nothing.
3. **Reproducible runs.** Every run records parameters, feature list, code version,
   data cut-off, a hash of the input series and metrics, and writes predictions
   beside them.

On the synthetic **CRUDEOIL** series, mean skill against the baseline is slightly
negative while roughly half the folds beat it — ten of twenty-one on the run that
produced the figures in `docs/figures/` — because a few bad folds dominate the
average. That is the honest result and the interesting one: it is why you look at
the distribution of folds rather than a headline number, and why the run's
verdict reports the count alongside the mean instead of only the mean.

## Running against the real sources

The synthetic generator exists so the demo cannot fail on a network. For real data:

```bash
python -m mdp.cli ingest --source binance_trades          # public market data
python -m mdp.cli ingest --source mcx_bhavcopy            # published end-of-day file
```

MCX intraday and tick products are licensed and are not fetched here; the contract's
`lineage` block says so. If the exchange endpoint is unreachable, drop the file in
and pass `local_file`, which is also how a vendor SFTP delivery would arrive.

### On a server instead of DuckDB

```bash
docker compose up -d
export MDP_STORE=questdb       # applies sql/02_questdb.sql
python -m mdp.cli init
python -m mdp.cli ingest --synthetic
```

`MDP_STORE` is the whole of what a deployment configures, and `questdb` and
`duckdb` are the two values it takes. The default stays `duckdb` so that a clone
runs with nothing installed; production names its store explicitly, which is
better than a default that is right for only one of the two audiences.

## What I would do differently at their scale

At 1 TB a day the shape of this changes in specific ways, not in principle:

- **Capture and parse become separate processes.** The capture writes bytes with a
  timestamp and does nothing else, so a slow parser can never drop a message.
- **Inserts get bigger and rarer.** Large batches, one writer per partition, and
  back pressure that is visible rather than implicit.
- **Partitioning gets stricter** and cold data ages out to object storage on a TTL,
  queried in place when someone needs history.
- **The contract gate moves off the critical path** and runs on the way from raw to
  served, so validation cost never delays capture.
- **The Python client stays**, because the interface a researcher uses should not
  change when the storage underneath it does.

## Layout

```
config/sources/*.yml          one file per source: adding a feed is this
config/contracts/*.json       schema and rules each dataset promises
config/calendars/mcx.yml      trading days, holidays, publication deadline
config/contract_specs.yml     lot and tick sizes, versioned by the date they took effect
config/entitlements.yml       who may read which dataset, and under what licence
config/models/*.yml           model recipes: inputs, features, validation, scenarios
config/vendor_quality.yml     vendor scoring weights and thresholds (see the note below)
sql/02_questdb.sql            the primary store: designated timestamps, DEDUP grains, partitioning
bench/gen.py                  the benchmark dataset: one file, loaded by every engine
bench/RESULTS.md              the as-of join measured on three engines, and what it does not measure
src/mdp/config.py             source configs, contracts, secret resolution
src/mdp/acquire.py            fetching, retries, synthetic generator
src/mdp/contracts.py          the validation gate
src/mdp/landing.py            raw and quarantine zones, and retention
src/mdp/storage.py            QuestDB and DuckDB behind one interface
src/mdp/quality.py            load checks and vendor scores
src/mdp/reference.py          expiries, front-month map, continuous series
src/mdp/serve.py              the client a quant developer imports
src/mdp/model.py              features, walk-forward, baseline
src/mdp/recipes.py            recipes and isolated scenario runs
src/mdp/runs.py               run registry
src/mdp/charts.py             figures for the deck, drawn from run output
src/mdp/calendar.py           trading days, holidays, and when a file is actually due
src/mdp/fetch.py              automated retrieval: pull the file, land it atomically
src/mdp/watch.py              event-driven ingestion: arrival triggers the load
src/mdp/arrival.py            waiting for a file in its window: on time, late, or missing
src/mdp/research.py           the as-of join: what was true at this instant, and nothing after
src/mdp/cursor.py             high-water marks: where each source was read up to
src/mdp/monitor.py            the data that never arrived
src/mdp/entitlements.py       who may read what, and a record of who did
src/mdp/metrics.py            Prometheus output, and the exporter that serves it
src/mdp/alerts.py             who gets told, and how loudly
src/mdp/pipeline.py           the whole thing, in order
src/mdp/cli.py                every command in the tables above, and the hand-run path that a
                              test pins against the Dagster code location, so the two cannot
                              drive different pipelines
flows/definitions.py          the Dagster code location: assets, checks, trading-day partitions
flows/dagster.yaml            the instance config that holds the writer pool at one slot
scripts/alert_sink.py         stands in for Slack or PagerDuty
scripts/drop_sample.py        drops a vendor-shaped file into the watched directory
monitoring/rules/mdp.yml      alert rules, in version control next to the scrape config
monitoring/tests/mdp_test.yml unit tests over those rules: parsing is not behaviour
monitoring/grafana/           provisioned dashboard and datasource
notebooks/research.ipynb      the research surface, executed on render
docs/ARCHITECTURE.md          why every choice was made, and what was rejected
tests/test_platform.py        the failures that would otherwise be silent
tests/test_audit_fixes.py     regression tests, one per defect found in the audit
tests/test_orchestration.py   what the Dagster code location claims, asserted in-process
tests/test_questdb.py         the store's behaviour, asserted against a live QuestDB
docker-compose.yml            QuestDB, version-pinned, plus the monitoring profile
pyproject.toml                dependencies, ruff, mypy, pytest — one file, no setup.cfg
uv.lock                       the pinned resolution CI and the image both install from
```

**A note on `config/vendor_quality.yml`.** Everything in it is read on every load:
the weights, the per-source freshness targets and the green/amber thresholds
behind the daily vendor score. What is *not* in it is any ranking of sources per
field, because the platform cannot arbitrate between sources and declaring that it
could would be a claim a reader can check in thirty seconds. Arbitration needs a
`source` column on the fact tables so a row can say where it came from, and then a
resolver that records which source won. The trigger for building it is specific: a
second vendor for a dataset that already has one. `docs/ARCHITECTURE.md` states
the gap in the same terms.

## The parts people usually leave out

| | What it does |
| --- | --- |
| **Entitlements** | Licensed data has an allow list per dataset, enforced at the serving layer and logged either way, because the interesting audit question is who *tried*. The platform's own operational tables — catalogue, check history, vendor scores and the access log itself — are a dataset too, and covered by the same policy |
| **Contract specs, point in time** | A lot size that changed at the start of 2020 means the same price is a different notional. ₹6,200 is ₹620,000 a lot today and ₹310,000 in 2017. `notional` raises when no spec version covers the date, rather than assuming a multiplier of one. (The CRUDEOIL history in `config/contract_specs.yml` is illustrative, and says so, so the point-in-time lookup has something to find) |
| **Schema versions** | A contract version change stops the load until a person accepts it. A provider changing the shape of a dataset at 6am should not quietly rewrite what consumers think they are reading. A gate that cannot read its own state blocks rather than treating the failure as a first sighting |
| **Restore** | `make restore` rebuilds the database from the raw files, through the same contract gate, and a test asserts the rebuilt row count accounts for every row in the landing zone — restored plus rejected. It validates *before* it truncates, so a bad landing file leaves the existing table alone and the event is recorded as `fail`. A backup nobody restores is a hope |
| **Secrets** | `${env:VAR}` in config, resolved at load, redacted in anything printed. A missing variable fails at startup, not as a confusing 401 at 6am |
| **Retention** | Raw files age out of the landing zone on a policy; quarantine never does, because it is the evidence in a vendor conversation |
| **Metrics** | Row counts, data age, check outcomes, vendor scores, cursor position age *and* cursor idle time, and calendar state, scraped by Prometheus. The last two are separate metrics because they answer different questions and one of them was answering the other's |
| **Alerting** | Rules in version control next to the scrape config, with unit tests over them; pipeline events pushed to the same Alertmanager; severity maps to the decision, not the surprise |

## Where the output goes

Two surfaces, deliberately separate, because they answer different questions for
different people.

### Operations: Prometheus, Alertmanager, Grafana

```bash
make exporter      # in one shell: serves /metrics on :9108
make monitoring    # in another: brings up the stack
make alert-sink    # in a third: prints what Alertmanager routed
make alert-test    # push a pipeline alert through the real path
```

Grafana opens on the provisioned dashboard at <http://localhost:3000>. Rules are
at <http://localhost:9090/alerts>, routing at <http://localhost:9093>.

#### Without Docker

`monitoring/` addresses its neighbours by compose service name, so on a machine
running the three binaries directly those names do not resolve. `make
monitoring-local` renders a variant that uses `127.0.0.1` and absolute paths, and
prints the three commands to start against it:

```bash
make monitoring-local
```

It is a renderer rather than a second set of files on purpose. This repository
grew the hand-edited copy first, in a folder beside it, and the two drifted the
same day: a corrected Grafana panel sat in `monitoring/` for hours while the
running Grafana served the old one, with no error anywhere to say so. Everything
that decides behaviour — scrape intervals, rule files, routing, grouping,
thresholds, panels — now has one definition, and the rendered config differs from
it only in addresses. `tests/test_monitoring_local.py` asserts that: any line
that changes without carrying an address fails the build, and one of its cases
feeds the checker a substitution that retunes `scrape_interval` to prove the
check can fail. The local Prometheus globs `monitoring/rules/*.yml` in the
repository itself, so the thresholds are shared rather than copied, and Grafana
provisions from `monitoring/grafana/dashboards` for the same reason.

Three properties of that stack decide it, and none of them is about who else runs
it:

- **The pull model means the exporter is stateless and the monitoring outlives the
  application.** The platform serves numbers on an endpoint and holds no
  connection, no queue and no delivery state; Prometheus decides when to look. So
  the monitoring keeps working when the pipeline is the thing that has died, which
  is exactly when it is needed.
- **PromQL is built to alert on a trend rather than on a sample.**
  `increase(mdp_checks_total{status="fail"}[15m]) > 0` says "a check failed at some
  point in the last quarter of an hour", and
  `increase(mdp_checks_total{status="warn"}[1h]) > 5 for: 30m` says "warnings are
  accumulating and have not stopped" — which no single sample can say. Every rule in
  `monitoring/rules/mdp.yml` is one of those two shapes, and reproducing them over a
  status table means reimplementing range vectors.
- **Alertmanager separates detection from routing.** Grouping, silencing,
  inhibition and the routing tree are the parts that decide whether alerts keep
  being read, and they are configuration rather than code in the pipeline. That is
  why `alerts.py` is small: it decides severity and a stable `alertname`, and
  nothing else.

A data platform that ships its own bespoke monitoring UI also asks a team to watch
two screens during an incident, and the second one is always the one nobody opens.

Four things about the exporter are deliberate:

- **It runs alongside the pipeline, not inside the compose file**, for the same
  reason `node_exporter` does not live in an application's stack: it has to keep
  answering when the application is the thing that has died. Shared lifecycles
  make "the platform is down" and "the monitoring is down" one event, and the
  alert you most need becomes the one you cannot get.
- **Each scrape opens and closes its own read-only connection.** A long-lived
  reader would fight the daily load for the DuckDB file lock, and the thing
  watching the pipeline must never be why the pipeline stalled. Read-only because
  the observer should be *incapable* of writing to what it measures, not merely
  disinclined.
- **A failed scrape serves the last good payload**, plus a failure counter and a
  last-success timestamp. "The endpoint is down" and "the endpoint is up but the
  numbers are frozen" are different incidents that get confused constantly, and
  they have their own alerts.
- **One probe query sits outside the per-section error handling.** Every section
  swallows its own exception, which is right for a table that does not exist yet
  on a fresh install. But those handlers would also turn an unreachable database
  into a clean, well-formed scrape containing no metrics at all — and an absent
  metric fires no alert. A monitoring endpoint that reports health by saying
  nothing is the worst way for monitoring to fail, so `SELECT 1` runs first and
  is allowed to raise.

Alerts arrive from two directions and meet in one place:

| Path | For | Example |
| --- | --- | --- |
| Prometheus rules | Trends and thresholds over time | Vendor score under 0.8 for an hour; cursor stopped advancing; file past its deadline |
| Pushed by the pipeline | Discrete events the metrics cannot reconstruct | Load blocked because `settle` went missing; 412 rows quarantined on a cross-field rule; the front-month map restated for a day already served |

Both land in Alertmanager rather than at their own destinations, and that is the
decision worth defending. Posting straight to Slack from the pipeline works right
up until the afternoon somebody needs to mute a known-bad vendor and finds there
is nothing to mute: no grouping, so a bad morning is one message per symbol; no
silences; no inhibition, so a root cause and its twenty consequences all page
separately; and two rotations to configure instead of one. One grouping policy,
one silence list, one rotation.

Every staleness rule is gated on the trading calendar — but by measuring lateness
against the delivery deadline rather than by asking whether the market is open
right now. The obvious gate is wrong in the other direction: it makes a Friday
file that never arrives unpageable until Monday night. An alert that fires each
weekend gets muted, and a muted alert looks like coverage while providing none;
an alert that cannot fire over a weekend is worse.

### Research: a notebook

```bash
make notebook     # executes it and writes docs/research.html
make lab          # opens it in Jupyter
```

A notebook earns its place here for one reason: the work it holds is a sequence of
questions where the answer to each decides the next, and it keeps the call, the
number and the reasoning in one artefact that can be re-run. It is also the only
consumer of the serving API that is not a test, so it is what catches an interface
that is technically correct and unusable. Every number in it comes through
`MarketData`: no file reads, no SQL, no knowledge of where the parquet lives. A test greps the committed cells for `read_parquet`, `SELECT `,
`store.query(`, `duckdb.connect` and `open(` and fails if any of them appear —
worth having as a tripwire, because a shortcut taken once gets copied, but it is
a substring check rather than a proof. The notebook does import `mdp.model`,
`mdp.recipes` and `mdp.charts`, which are library code sitting on the same
serving API rather than a way around it. It walks the roll and what
back-adjustment fixes, point-in-time reads, the as-of join on the tick path, the
feature registry, and the walk-forward comparison.

It is executed on render rather than committed with saved outputs. A notebook
whose stored output does not match its code is worse than no notebook.

There is no BI layer and that is the design, not an omission. The consumer of a
market data platform is code. The serving API is the product; the notebook is one
caller of it, and Grafana exists so somebody can see the platform is alive.

```bash
make charts       # PNGs into docs/figures/, for slides
```

Drawn with matplotlib, which fits the output rather than the fashion: these are
PNGs for a slide and figures in an executed notebook, so they need to be static,
deterministic and regenerable by a CI job with no browser and no JavaScript
runtime in the loop. A plotting library that renders in a browser would buy
interactivity that a PNG cannot carry anyway, and cost the ability to produce the
file headlessly.

Colours follow a fixed categorical order chosen for separation under the common
colour-vision deficiencies rather than by eye, and status colours are reserved
for state. No validator in the repo asserts that separation, so it is a rule
somebody followed rather than one the tests hold to.

## Tests

```bash
uv run pytest            # or: python -m pytest tests -q
```

Four files, and the split is by what a failure would mean.

`test_platform.py` holds one test per way market data goes wrong quietly: a
dropped column, a replayed trade, a clock ahead of ours, an impossible bar, a
stale feed, a roll that reverses, a feature that sees its own future.
`test_audit_fixes.py` holds one per defect found when the platform was audited
against its own documentation — a roll day with no print from the expiring
contract, a schema gate that failed open, a check that reconciled a file against
itself, a declared threshold that was evaluated and then ignored, an arrival
monitor that aggregated across symbols. Each is named for the failure rather than
for the function, so a red one says what broke.

`test_orchestration.py` asserts what the Dagster code location claims, and it
needs no Dagster instance, no daemon and no webserver: the partition set, the asset
graph and the check declarations are in-process properties of the definitions,
which is one of the better arguments for the asset model. Half of it parses
`flows/` and `src/mdp/` as text and runs with nothing installed, so the claims that
the code location and the CLI drive the same entry points and that no orchestrator
is a runtime dependency keep being checked in the environment where Dagster is
deliberately absent — which is the environment CI runs. That half, together with
`test_no_pipeline_code_imports_an_orchestrator` in `test_audit_fixes.py`, is the
whole of the portability claim; it used to be made by a second runner, and a static
parse is both stronger and cheaper to keep true.

`test_questdb.py` runs only against a live QuestDB and skips cleanly without one.
That is deliberate and not a compromise: what `ASOF JOIN` matches at a boundary,
whether `DEDUP` collapses at commit or eventually, what `SAMPLE BY` labels a
bucket and how a nanosecond timestamp lands in a microsecond column cannot be
asserted against a mock, because a mock asserts what somebody believed the server
does. Every one of those was wrong on the first attempt.

CI runs the suite on 3.12 and 3.13, then runs the demo and executes the notebook,
because a README that does not run is worse than no README. It also stands up the
pinned QuestDB as a service container, applies `sql/02_questdb.sql` to it and runs
both the CLI and `test_questdb.py` against it. That job blocks, with no
`continue-on-error` anywhere in the workflow, and the reason is a mistake this
repository already made: the ClickHouse schema it used to carry was the production
target for months, had never been applied to a server, and failed on its third
statement the first time anyone ran it because `toYYYY` is not a ClickHouse
function. A claim nobody runs is not a claim, which is why the store the platform
actually depends on does not get a job that is allowed to be red.

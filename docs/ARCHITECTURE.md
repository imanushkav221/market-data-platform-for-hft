# Architecture and the reasoning behind it

Every choice here is answerable with a "because", an alternative that was
considered, and what would change on a real desk. Where a choice was made for the
demo rather than for production, it says so.

---

## 1. Start from the requirements, not the tools

A market data platform for a trading desk has to satisfy five things. Everything
below follows from them.

| Requirement | What it actually means |
| --- | --- |
| **Correct** | A number the desk sees matches the exchange. Wrong data is worse than missing data, because missing data is visible |
| **On time** | The desk needs it before it is useful, not eventually. Minimum delivery latency, per the role |
| **Explainable** | When a number changes, somebody can say why, months later |
| **Extensible** | A new instrument or vendor is a normal day, not a project |
| **Consumable** | A researcher gets data without learning the storage layer |

Nothing here is about scale for its own sake. At this size, correctness and
explainability are the hard parts; volume becomes the hard part later, and section
7 covers what changes when it does.

---

## 1b. Two shapes, both complete

A desk has an end-of-day file and a live feed. The temptation is to build the
first properly and leave the second at ingestion, which is how a platform ends up
with a tick table nobody consumes. Both paths here go the whole distance —
arrival, validation, storage, reference, consumer — and differ in mechanics
rather than in completeness:

| Stage | Medium frequency | High frequency |
| --- | --- | --- |
| Arrival | one publication a day, waited for in a window | continuous, cursor-driven polling |
| Validation | cross-field rules, coverage against the trading calendar | replay duplicates, ordering, clock skew |
| Storage | contract-day grain, yearly partitions, corrections kept beside what they correct | daily partitions, batched inserts, the grain deduplicated at commit, bars aggregated on read |
| Reference | expiries, roll, lot size, point-in-time | none |
| Consumer | back-adjusted series → model | bars plus as-of join → research rows |

The asymmetry that remains is honest and worth stating: the tick feed is a public
venue, because exchange tick data is licensed and cannot be had for a
demonstration. The design does not change with the source.

## 2. The shape

```
  vendors / exchanges           the platform                         consumers
  ------------------           -------------                        ---------
  MCX published EOD   ->  acquire  ->  contract gate  ->  landing  ->   QuestDB   ->  Python client
  Binance public API      (retries)    (quarantine)       (Parquet)   (SAMPLE BY)      get_bars()
                              |             |                              |            get_continuous()
                              |             |                         reference          |
                              +-------------+------> quality checks <-----+               v
                                                     vendor scores                    research,
                                                     run registry                     model runs
         orchestration: Dagster assets - and nothing above imports Dagster
```

Five layers, each replaceable without touching the others: **acquire**, **validate**,
**store**, **serve**, **orchestrate**. That separation is the single most important
property of the design, because every one of these gets replaced eventually — the
vendor changes, the database changes, the orchestrator changes.

---

## 3. The decisions

### 3.1 QuestDB as the serving store, decided by measurement

**Because** one query decides this platform's read path and it was measured.
The as-of join — "the last trade at or before each of N decision timestamps" — is
what the platform exists to make correct; bars, freshness and the roll are either
cheap or rare. `bench/` is that measurement: one generated dataset of 20,000,000
trades over 5 symbols and 31 days, the same bytes loaded into every engine, on 2
cores and 7 GB. All three returned identical answers.

| Engine | 5,000 decisions | 50,000 decisions | Peak RSS from the join |
| --- | --- | --- | --- |
| **QuestDB 8.2.1** | **0.029 s** | **0.081 s** | **+297 MB** |
| ClickHouse 24.8.14 | 1.06 s | 1.06 s | +839 MB |
| DuckDB | 1.94 s | 2.15 s | 2,502 MB total |

**The shape matters more than the ratio, and it is the actual argument.**
ClickHouse's time did not move when the work went up tenfold, because its cost is
not in the query: it builds a hash table over the entire right-hand side, and
as-of semantics fix which side that is, so the whole 20M-row trades table goes
into memory every time. The measured +839 MB is about 42 bytes a row, which is
what the join columns come to. There is no spill-to-disk variant — `grace_hash`
excludes ASOF — and index-based narrowing for ASOF is
[an open issue](https://github.com/ClickHouse/ClickHouse/issues/38444). QuestDB
binary-searches a physically time-ordered column store, once per decision: 2.8x
the time for 10x the decisions, and +2 MB of resident memory for the extra 45,000
lookups. One cost grows with the table; the other grows with the question. At 20M
rows ClickHouse is slower and fine. The reason to choose is what happens at two
billion.

**What the benchmark does not say, stated plainly**, because a measurement
presented without its limits is a marketing claim:

- **One machine, 2 cores, 20M rows.** ClickHouse is built to scale across cores
  and nodes and this container gives it neither. A 16-core box would narrow the
  time gap; it would not change the memory slope, which is the finding.
- **Only the as-of join.** ClickHouse is better at the platform's other half:
  `argMax(..., ingested_at) GROUP BY grain` for point-in-time reads is cleaner and
  scales better than QuestDB's `LATEST ON` with several partition keys, which
  scans.
- **Nothing about operating either.** QuestDB's open-source build has no
  replication, no TLS and no role-based access control; all three are Enterprise.
  ClickHouse is Apache 2.0 with all of it included. For a platform serving
  licensed exchange data that is a real cost on the QuestDB side, and it is in
  none of the numbers above. It is also the one thing that would change this
  decision without any benchmark being re-run.

**A risk worth naming rather than arguing around:** fewer people have operated
QuestDB than ClickHouse, so a team is less likely to have someone who has debugged
a suspended WAL at 6am, and the answers are thinner when they go looking. That is
a cost of this choice. It is not an argument against it, and it is not an argument
for it either — which is why it is here and not in the paragraphs above.

**Alternatives rejected on merit:** PostgreSQL, which is fine until partition and
index maintenance dominate and every scan reads whole rows; a cloud warehouse such
as Snowflake or BigQuery, which is the right answer for enterprise analytics on a
schedule and the wrong one when a researcher iterates in seconds and per-query
cost is a consideration; MongoDB or a document store, which gives up exactly the
columnar reads the workload lives on.

**Cost if wrong:** the client library means consumers do not care, so the blast
radius of changing this decision later is one module — which is also what made
running the benchmark cheap enough to be worth doing.

### 3.2 ClickHouse measured, rejected, and what would bring it back

**Because** the benchmark decided the read path, and after it a second
server-backed implementation was carrying cost without earning any. ClickHouse was
the production target before `bench/` existed. Afterwards the repository still
carried a 183-line `ClickHouseStore`, a 353-line `sql/01_clickhouse.sql` and a CI
job marked `continue-on-error: true` — a repository stating in its own workflow
file that it did not maintain that path. All three are now deleted.

**The deletion does not touch the measurement.** `bench/` drives ClickHouse with
raw SQL over HTTP and never went through `ClickHouseStore`, so every number in
section 3.1 is exactly as reproducible as it was, `bench/run.sh` still stands
ClickHouse up and runs it, and the comparison stays in this document because the
comparison is the reason the current store was chosen.

**The other half of the argument is that three backends cost the interface.** A
`Store` over three engines can only express what all three have in common, which is
the opposite of what you want once a store has been chosen for its specific
strengths. `QuestDBStore` needs a WAL commit wait, a staged decisions table because
`/exec` is GET-only, a probe for a key with no rows, and a column-name rule for
days versus instants. Those are QuestDB facts, and the interface now has room to
hold them instead of averaging three engines.

**What would make ClickHouse primary again, kept as prose because it is still
true:**

- **Point-in-time reads.** `argMax(..., ingested_at) GROUP BY grain` is the natural
  shape of "the latest belief per grain". QuestDB's `LATEST ON` with several
  partition keys scans, which is why `_latest_per_grain` in `serve.py` resolves
  corrections in pandas on read instead. That is fine at the current end-of-day row
  counts and is the first thing that breaks if they grow.
- **Operating it.** Replication, TLS and role-based access control are in
  ClickHouse's Apache-2.0 build and are Enterprise-only on QuestDB. For a platform
  serving licensed exchange data that is a real cost, it appears in none of the
  benchmark numbers, and it is the one thing that could reverse this decision with
  no benchmark re-run at all.

**What the deletion costs, named rather than argued around:** a second
implementation is what keeps the storage boundary honest. Every place the pipeline
assumed a behaviour of one engine — a column default that fires, a write that is
visible to the next read, a `DATE` distinct from an instant — was found by making
another store work, not by intending to be portable. DuckDB still applies that
pressure (section 3.2a), but it is in-process, so it does not exercise the network
and commit-visibility class of assumption the way a second server did. The
mitigation is that the ClickHouse-specific reasoning — sort key as dedup key,
refreshable rather than insert-triggered views, the TTL path — is written down in
sections 3.8, 3.9 and 5b of this document rather than living only in a deleted file, so
rebuilding the backend would be a day's work rather than a rediscovery.

### 3.2a DuckDB as the second implementation

**Because** the demo, the tests and a new joiner's first command must run with no
server, no Docker and no network. `get_store()` therefore defaults to DuckDB even
though QuestDB is primary: production names its store in one environment variable,
and an explicit choice in the place that cares beats a default that is right for
only one of the two audiences.

**Honest framing:** this is a demo and test decision, not a production one.
DuckDB's file lock is exclusive and per process, so a reader collides with a
writer exactly as two writers do — which is why every asset in the Dagster code
location, including the read-only ones, takes the single-slot writer pool. That
constraint is expressed once, in the orchestrator, rather than remembered at each
call site.

### 3.2b What building the QuestDB backend found

A benchmark says which engine is faster at one query. It says nothing about what
the engine is like to depend on, and that only comes out of writing the backend
and running it against a live server. Eight things did, and each of them is in the
code with the measurement attached, because a finding written down as a general
worry is not usable by the next person.

- **`ASOF JOIN` degenerates on a join key that matches nothing.** Against the
  20M-row benchmark table the same 5,000 decisions answered in 0.062 s for a symbol
  that exists and hit the server's 60-second timeout for one that does not: the
  backward scan for a key that will never appear does not terminate at a partition
  boundary, so the cost stops being "a binary search per decision" and becomes "the
  table per decision". The practical shape of that is a typo — `as_of("BTCUSD",
  ...)`, one character short — taking a minute and then failing, where DuckDB
  returns a column of nulls immediately. `as_of` therefore probes the key
  first (2 ms when it matches, about 100 ms when it does not) and answers without
  the join. The answer is identical either way: every match would have been null.
- **`TOLERANCE` does not exist in 8.2.1, and it is the wrong tool regardless.** It
  arrives in 8.3 and would bound how far back the join looks, which reads like an
  exact fit for `max_staleness_seconds`. It is not: it makes a too-old match come
  back as NULL, and `as_of_prices` needs the match's timestamp precisely so it can
  report *how* stale the price it refused to return was. A hole with no age on it
  is a worse answer than the one the platform already gives. So the staleness bound
  stays outside the join even when the clause becomes available.
- **`SAMPLE BY ... FROM x` anchors the bucket grid on `x` rather than filtering
  it.** Asked for `SAMPLE BY 1m FROM '10:00:30' ALIGN TO CALENDAR` against trades
  from 10:00, the live server returned buckets stamped 10:00:30 and 10:01:30, and
  `ALIGN TO CALENDAR` did not override it. Those are real one-minute bars on a grid
  nobody asked for, and joining them to anything else on the platform would be
  joining bars that do not line up. Passing a caller's start time into `FROM` would
  silently relabel every bar in the result, so `bars()` widens the `ts` predicate to
  the enclosing bucket boundaries and filters the bucket in the outer query.
- **`/exec` is GET-only, and SQL dies past roughly 64 KB with no error.** A POST is
  answered with 404 "Method not supported", so the statement travels in the URL and
  is bounded by the server's request header buffer; past it the connection closes
  with no response, which arrives as a `ConnectionError` rather than as a SQL error.
  Measured at around 480 decision timestamps for the as-of query's shape. That is
  why `as_of` stages its timestamps in a table — 36 ms for three, 76 ms for 5,000 —
  instead of having one code path for small N and another for large, which is where
  a divergence hides.
- **`/imp` returns HTTP 200 `"status":"OK"` while silently dropping rows.**
  `atomicity=abort` does not change it: a CSV with one unparseable field imports the
  other row, answers OK, and reports `"rowsRejected":1` in the body. The loader
  reads that field and raises. A loader that trusts the status code loses rows
  quietly, which is the exact class of failure this platform exists to prevent.
- **A WAL write is acknowledged before it is readable.** It is durable at the
  sequencer and visible when a background job applies it, so `/imp` returning does
  not mean the next `SELECT` sees the rows. The platform depends on read-your-write
  in several places — the cursor is written and the next window reads it back, the
  schema gate writes a first sighting and the next run reads it — so `insert` waits
  for the apply job to catch up, and checks for a *suspended* WAL rather than
  polling a table that will never converge.
- **One temporal type, and the resolution is a smell.** QuestDB cannot distinguish
  a calendar day from an instant; DuckDB can, and the code above the storage layer
  was written against it — `reference.py` compares `trade_date`
  to a naive `pd.Timestamp`, and pandas raises rather than coercing. So the store
  resolves it by column name: a fixed set of names that mean a day everywhere in
  this schema come back tz-naive. It works, and it is a convention holding up a type
  system, which is worth naming as one rather than presenting as a design. The
  alternative — QuestDB's own `DATE` type — was tried and is worse, because a
  QuestDB `DATE` cannot be a designated timestamp, and `trade_date` is the column
  every read of `eod_bars` filters on.
- **`DEDUP` requires the designated timestamp in the key set**, which puts a hole in
  the trades table that a ClickHouse `ReplacingMergeTree` does not have. There the
  sorting key is `(venue, symbol, trade_id)` with `ts` deliberately outside it, so
  a resend that *corrects* a print's timestamp replaces the original. Here a
  corrected `ts` is a different dedup key, so the correction lands beside the
  original and both survive — and the open-source build has no row-level `DELETE` to
  retire the old one with, so the repair is a partition rewrite. It is the same class
  of hole as ClickHouse's "a correction that moves `ts` across a month boundary never
  collapses", and it is wider. Both are runbook entries; neither is invisible.

### 3.3 Parquet landing zone, written before validation

**Because** the raw bytes are the only thing that cannot be reconstructed. If a
transform is wrong, replay costs minutes; going back to a vendor for last month's
file costs days, and sometimes the file no longer exists. Parquet because it is
columnar, compressed, portable and readable by everything.

**Alternatives rejected:** loading straight to the database, which leaves no way
back and no evidence when a vendor disputes a number; keeping the vendor's raw CSV
or JSON, which is honest but 5 to 10 times the storage and slow to replay.

**At scale** this becomes object storage with a lifecycle policy, and the landing
zone stops being a directory and becomes a bucket. Nothing else changes.

### 3.4 Dagster for orchestration, and one orchestrator only

**Because the unit of work here is a dataset, and of the three candidates only one
declares that directly.** What exists in this platform is an end-of-day futures
table, a tick table, a derived front-month roll map, a back-adjusted continuous
series, contract specifications and model runs. `@asset` declares exactly that:
the thing that exists, the things it is computed from, and the code that computes
it, so the task graph falls out of the data graph rather than being maintained
beside it. In Airflow an asset is a URI string with no properties; in Prefect it
is an annotation hung off a task. That is the first reason; the three below follow
in order of their weight on this workload.

**The platform's data contracts *are* asset checks.** `run_source` already
validates against the contract before anything reaches the database, aborts on a
schema-level failure and quarantines row-level ones. Under Dagster that becomes a
property of the dataset instead of a red task somebody has to open and interpret:
`flows/definitions.py` declares the checks *from the source YAML*
(`cfg.quality["checks"]`, not a list in the orchestrator, which would quietly
falsify "a new dataset is a YAML file plus a JSON contract") and translates the
platform's own `QualityReport` into `AssetCheckResult`s. Nothing is
reimplemented, and severity comes from the platform's `pass`/`warn`/`fail` rather
than from a second list of which checks are fatal. Blocking works, and it was
verified rather than assumed: forcing a real schema-version failure produced no
materialisation event for the partition and no downstream execution.

**Trading-day partitions are one keyword argument.**
`TimeWindowPartitionsDefinition(..., exclusions=<the holidays from
load_calendar("mcx")>)` makes the partition set *be* the MCX calendar. A holiday
is not a partition that gets skipped; it is not a partition, so
`2026-10-02` cannot be materialised because it does not exist, which is a stronger
statement than a job that checks the calendar and returns early. Verified across
all of 2026: nothing in the set falls on a non-trading day, and no trading day in
the covered range is missing. A backfill of `2026-09-10..2026-09-18` produces
exactly six materialisations for a nine-day range — the weekend and Ganesh
Chaturthi are absent from the range because they are absent from the partition
set, with no calendar lookup in the loop.

**Secondary but real: the framework computes the data version.**
`data_version = hash(code_version, input data versions)` is a framework invariant
in Dagster and is hand-rolled in `runs.py` and `recipes.py` (`code_version()`,
`fingerprint()`). Those stay, because the platform has to answer "why did this
number change?" with no orchestrator installed — but the assets hand Dagster the
platform's own hashes, so the two agree rather than compete.

**Prefect ranked last on this workload and has been deleted**, not left next door
as an option. It loses on the two properties above that carry the most weight here:
no partitions of any kind, so "backfill a range" and "replay one day" are `for`
loops you write and maintain, and no checks, so a blocked load is a raised
exception and "blocked" versus "broken" is a string in a log line. Its Assets UI is
Cloud-only, so the one feature that would have narrowed the gap is unavailable to a
self-hosted deployment. Retiring an orchestrator means deleting it — a test asserts
the file is gone — because a runner nobody maintains is a worse claim than no
runner.

**Airflow is the close second, and the thing it does better than Dagster is worth
naming.** Its data-interval and catchup model is genuinely good — arguably better
than Dagster's for this shape of source. Airflow hands each run a
`data_interval_start` and `data_interval_end` and replays a range of them, so "load
exactly this window" is the framework's own vocabulary rather than something the
asset has to derive from a partition key and interpret. AIP-76 partitioning and the
mapper algebra built on it close most of the remaining gap to the asset model, and
they are real rather than a roadmap. **What would make me pick it:** a deployment
that already runs a scheduler and a metadata database, a team that has operated it,
or a graph whose unit really is a task rather than a dataset. Any of those three
outweighs everything in this section; none of them is true here, where the unit is a
dataset and there is no incumbent scheduler to inherit.

This repository used to carry a DAG over these same functions, on the argument that
Airflow is what a firm probably already runs. That argument is reasoning from what
other people use, which is the reasoning the rest of this document exists to stop
doing, and the DAG did not survive its own comparison. The code location declares
five assets, sixteen asset checks — fourteen per-load ones read out of the source
YAML plus the two monitor checks that have something to say when nothing ran — four
jobs and four schedules, with the data contracts expressed as *blocking* checks and
the partition set carrying the exchange's holidays. The DAG declared one DAG and
five tasks, with no asset checks and no holiday exclusions. "Two runners of the same
pipeline" was a stretch, and inviting the comparison invited somebody to make it. One thing the port did catch, and it is kept
because it is a fact about Airflow rather than about the file: Airflow 3 made
`logical_date` optional, so a *manually* triggered run has no data interval at all,
and reading that absence as an empty window loads nothing and gets the load
correctly blocked by the contract — which looks like a pipeline failure and is a
misread of the run context.

**The portability claim is stronger than it was, because a test makes it now
instead of a second implementation.** Nothing under `src/mdp` imports an
orchestrator; `tests/test_audit_fixes.py` parses every module in the package on
every CI run to assert it, and asserts that no orchestrator is even importable in
the environment `uv sync --frozen` produces. The pipeline's entry points are
ordinary functions — `run_source`, `build_reference`, `await_arrival` — and
`src/mdp/cli.py` drives the same ones the code location does, which a second test
asserts, so the orchestrated path and the path a person types cannot diverge.

That is a better answer to "how long would porting this to Airflow take?" than a
half-equivalent DAG, on two counts. It is checkable: a static parse of every module
is either true or red, whereas a second runner is only as honest as the last person
who ran it. And it cannot quietly diverge, which a second runner demonstrably does
— the shell script this repository also used to carry broke silently when the
Prefect flow it invoked was deleted, and had to be rewritten before anybody noticed
it had been dead for weeks. The right question to ask of a portability claim is what
would falsify it, and the answer here is a named test rather than an invitation to
diff two files.

The single-writer constraint shows why the port is cheap. DuckDB allows one
connection to hold the file, and Dagster expresses that as `pool=` on every asset
with the limit in `flows/dagster.yaml`. Airflow's idiom for the same fact is a pool
with one slot. Both are deployment configuration rather than pipeline code, and both
hold *across* runs — which is what the Prefect flow could not express, because it
branched on the store's class name inside the flow function, leaving a second flow
run, or the CLI, or somebody's notebook free to open the same file and collide. What
no orchestrator can express is a limit that also binds the CLI, which is why the
constraint is written into the schema as well.

**What is given up by having one runner, stated plainly.** There is no longer a
cron-and-`flock` path in the repository for a box that has Python and a crontab and
nothing else. That path was real and it worked, and what replaced it is not an
orchestrator-free runner but the observation that every step it ran is an `mdp`
subcommand: five lines of crontab reproduce it, and the arguments *for* an
orchestrator that it used to make by omission — retries with backoff on the one step
that talks to somebody else's server, concurrency across independent sources, run
history to look at three days later, and a partition model so a backfill is not a
loop somebody writes — are the reasons this platform has one.

**One source, one load path, stated explicitly.** The platform has two ways into
the end-of-day table: `run_source` acquires a named day, and `await_arrival` polls
from the publication deadline and loads the file the moment it lands. The Prefect
flow ran both for the same source on the same day, which meant a second HTTP fetch
against the exchange in live mode and, in synthetic mode, a regeneration of the
entire history — which in turn made a daily cycle look as though it had built three
years of data. A daily cycle loads a day; history comes from a backfill, and when
there is not enough of it for a walk-forward the run says so and names the command.
In the Dagster code location the two paths are structurally separate: `eod_bars` is
the first, the arrival window is an op in its own job, and the routing rule is
written where both are defined — run one or the other for a given day, never both.

### 3.4a What building the Dagster code location found

The same rule as the store: the reasons above are properties of the model, and
these are what only appeared on the way to a working deployment. The first three are
cases where the obvious implementation is quietly wrong, the fourth is a limit of the
partition model meeting a property of this platform, and the last is an
inconsistency that is better flagged than left to be noticed.

- **A blocking check only blocks if the asset yields no output.** With a
  materialisation yielded, Dagster logs it and *then* fails the step, so a load the
  contract refused shows in the catalogue as materialised with a red check next to
  it — which states the opposite of what happened and is the one thing asset checks
  were adopted for. `output_required=False` is what makes "blocked" mean no
  materialisation event at all, and downstream assets are then skipped rather than
  failed: rebuilding the roll map from data that did not land is work with a wrong
  answer at the end of it.
- **`build_schedule_from_partitioned_job` drops `exclusions`.** It derives its cron
  from the partitions definition and produced `0 0 * * 1,2,3,4,5` here — which fires
  on the morning after a holiday as well as on the morning of one. Concretely, with
  Gandhi Jayanti on Friday 2026-10-02: the tick on Friday correctly targets
  2026-10-01, and the tick on Monday 2026-10-05 targets 2026-10-01 *again*, because
  10-02 is not a partition and 10-05 has not closed yet. That is a second load of a
  day already loaded. The schedule is therefore written out explicitly and asks the
  instance whether the partition it is about to request has already been
  materialised. Two consequences, both deliberate: a Friday partition is picked up
  on Monday rather than at the weekend, and anything older than the last closed
  partition is not chased — catching up a week is a backfill, which over a partition
  set that already excludes the holidays is one command with no calendar logic in it.
- **`exclusions` extends the preceding partition's time window rather than leaving a
  gap**, so an excluded day is absorbed into the partition before it. That makes the
  partition *key* the only safe source of the trading day: `previous_trading_day()`
  would answer 2026-09-25 on a Sunday while the window of the 09-25 partition runs to
  Monday 00:00 IST, so anything deriving the day from a timestamp is off by one
  exactly when a holiday or a weekend is involved. The partition set is the authority
  on what exists, so it is the thing asked.
- **`await_arrival` could not be a partitioned asset**, and the reason is a property
  of the platform rather than of Dagster. It loads by calling the watcher, which
  processes *every* unsettled file in the drop directory — correct for a watcher, and
  the reason the same path works whether we fetched the file, a vendor pushed it, or
  somebody dropped it there during an incident. So a materialisation of one day's
  partition can legitimately load three, and a partition that claims to be one day
  and is not is worse than no partition. Observed rather than theorised: asking for
  2026-09-25 reported 10 rows loaded with `published=False`, because one pending
  delivery published and another was blocked. It is an op in its own job, emitting an
  `AssetMaterialization` for the day it waited for, and carrying no check results —
  an op cannot emit them, and inventing per-check results for a report it never saw
  would be worse than pointing at the `quality_events` rows the platform did write.
- **One inconsistency, flagged rather than hidden:** `flows/definitions.py` is the
  only module here without `from __future__ import annotations`, because Dagster
  inspects the *runtime* type of an asset's `context` parameter and PEP 563 turns
  that annotation into a string it rejects. The alternative was leaving `context`
  unannotated, which loses the annotation a reader most wants.

### 3.4b Event-driven ingestion, and what it cost to build properly

**Because** a schedule is latency you chose. A file landing at 06:02 into a 07:00
pipeline is 58 minutes of stale data that nobody can see. The watcher ingests on
arrival and writes the measured latency — file timestamp to queryable — into the
same table as every other check.

**The implementation is a polling watcher**, because that runs anywhere with no
dependencies. In production the doorbell changes to an object-created event, an
inotify watch or an SFTP hook; everything after the trigger is identical, which is
why the watcher decides only *when*, never *what*.

**Three things that look like details and are not**, each one found by building it
rather than describing it:

- **Partial writes.** A large upload in progress looks exactly like a small
  complete file. The watcher waits for size and mtime to stop changing. Loading
  half a file is worse than loading none.
- **Re-delivery.** Vendors resend yesterday's file under today's name. Keying the
  ledger on filename let that through and double-loaded; identity is now a
  streamed content hash, with name, size and mtime as the fallback above a size
  threshold, and the database grain as the second line of defence.
- **A delivery that cannot be read.** The first version let a malformed CSV raise
  out of the loop and take every other source down with it. Now it is logged as a
  quality event, retried up to a budget, and then left for a person, because
  retrying a permanently broken file every two seconds is noise, not resilience.

### 3.4c Automated retrieval, decoupled from loading

**Because** a pipeline that waits for a human to put a file somewhere is a
demonstration, not a pipeline. The fetcher pulls from the exchange and lands the
payload in the drop zone; the watcher takes it from there.

**The indirection is the design.** The loader cannot tell whether a file arrived
because we fetched it, because a vendor pushed it over SFTP, or because somebody
dropped it in by hand during an incident. One path, three ways in. Replacing the
fetcher — new vendor, new protocol, an object-storage event instead of a poll —
touches nothing downstream, and the incident path is the same code as the happy
path, which is when you most want it to be.

**Writes are atomic**: content goes to a `.part` file and is renamed into place,
so a reader can never catch a half-written file.

**A skipped fetch says why it was skipped, and the order of those reasons is not
arbitrary.** The fetcher checks "is this date in the future" before it consults
the calendar, because a date that is both in the future and a Saturday is in the
future: that is the more fundamental reason and the one a caller needs to hear.
With the calendar first, `today + 3` reported "weekend" whenever today happened
to be a Wednesday or a Thursday — which also made the test pinning this behaviour
fail two days in seven, and a test that goes red on a schedule teaches whoever
owns it to ignore red.

### 3.4d A trading calendar, because silence has to mean something

**Because** the cheapest way to make a platform untrustworthy is to alert on a
Saturday. People stop reading the alerts, and then they miss the Tuesday that
mattered. The calendar decides which days a delivery is expected, when it is late,
and which days a backfill should cover.

**What it caught immediately:** a wall-clock freshness threshold is wrong for any
end-of-day file. Thursday's file is published after Thursday closes, so on Friday
morning it is already fifteen hours old and on Monday it is three days old with
nothing wrong. Freshness is now calendar-aware: not "how old is this?" but "is
this the most recent trading day that should have been published?"

**And a second thing:** the rule that is right for the daily run is wrong for a
backfill. March data is not current and never will be, so applying freshness to it
would block every historical load and teach people to pass `--force` around a
safety check, which is how a check becomes decoration. A load now knows whether it
is live or a backfill, and dataset currency is covered separately.

### 3.4e The data that never arrived

**Because** every check in `quality.py` looks at data we received, and none of
them can see the file that was never sent. That is the failure that actually costs
money: the desk queries yesterday's numbers all morning because nothing broke
loudly enough to notice.

The arrival monitor asks the opposite question against the calendar — what should
be here and is not — and pages when a trading day is past its deadline with
nothing loaded. Gaps are written to the same table as every other quality signal,
so "was Tuesday ever loaded?" has one place to look whether the answer is yes, yes
but late, or no.

**Per symbol, because a day-level row count answers a question nobody has.** The
source declares five MCX symbols. Counting rows per day meant three trading days
of entirely absent CRUDEOIL reported no gaps at all, as long as GOLD arrived —
which is the exact shape of the failure this module exists to catch. The monitor
now distinguishes a day where *nothing* arrived from a day where *some symbols*
are missing, and the two get different alertnames so Alertmanager groups them
separately and whoever is woken up knows which they are looking at before opening
anything. Both page. An incomplete day is not the lesser problem: the dataset
looks populated and every query over it silently returns a subset. A day still
produces at most one alert, with the missing symbols named on it, because one
incident should page once rather than five times.

**Alerting has three levels and one rule:** page on data that is wrong or missing,
notify on data that is merely imperfect, never alert on a closed market. The sink
is a log or a webhook and cannot fail the pipeline, because an alerting system
that can take down what it monitors is worse than none.

### 3.4f How often it fetches, and how it knows what to ask for

| Source | Cadence | Mechanism |
| --- | --- | --- |
| End-of-day file | polls from the 23:55 deadline until it lands, up to two hours | One publication a day, and nothing exists to fetch until the session closes. Publication time varies, so it waits inside a window |
| Continuous venue | every 60 seconds | A cursor: everything since the last successful load, plus a five second overlap |

**Why a cursor rather than a window.** A fixed window is wrong in both
directions. Too small and a late or slow run loses data silently. Too large and
every run re-reads the same rows, which is invisible until the source starts
rate-limiting at the worst possible moment. Three rules make the cursor safe:

- it moves **only after a successful publish**, so a blocked load leaves the
  position alone and the next run picks up the same data;
- it moves to **what actually loaded**, not what was requested, because those
  differ whenever a source truncates a response;
- it **overlaps deliberately**, because re-reading five seconds costs one
  deduplicated write and missing five seconds is silent and permanent;
- it is **capped at the end of the window that was asked for**, which is the rule
  that was missing and the one that mattered most.

That last rule exists because of a hole that produced exactly the permanent
silent gap the other three promise cannot happen. A single print carrying a
clock-skewed timestamp two hours ahead of the requested window dragged the
high-water mark along with it. The next window then started after it ended, read
backwards, and the two hours in between were never requested by any run, ever.
Not an error, not a retry: just missing, with nothing in the logs. Capping is the
right response rather than rejecting the row, because the skewed print is real
data that belongs in the table — what it must not do is speak for time the
pipeline has not yet covered.

**Why a window and not a fixed time.** A single scheduled fetch assumes the
exchange publishes exactly when it says. It usually does, and the days it does not
are the days that matter: the file lands forty minutes late, the fixed job already
ran and found nothing, and the gap surfaces when a researcher asks why yesterday
is missing. A window gives three outcomes instead of one guess — on time, late
(loads anyway, delay recorded, warning raised), or missing (window closed, page) —
and the difference between the last two demands completely different responses
while looking identical to a single-shot fetch.

**Both cadences live in config.** Polling a venue at sixty seconds is a choice
about rate limits and acceptable loss, not a property of the code. Streaming over
a websocket is the next step for the continuous path and changes one driver plus
one config file.

### 3.4g The as-of join, and the two ways it is usually wrong

**Because** "what was the price at this moment?" is the most common question a
research desk asks a platform, and the two obvious implementations are both
subtly wrong.

**Nearest-timestamp matching** returns prints from after the decision moment. It
is the single most effective way to produce a backtest that looks excellent and
cannot be traded. The join here is strictly at-or-before, and a test inserts a
print 200 milliseconds after a query time and asserts the answer does not change.

**Silently returning whatever the last trade was**, however old, turns a data gap
into a plausible number. Past an explicit staleness bound the row survives and the
price comes back empty, so the consumer has to deal with the hole. A NULL somebody
must handle beats a stale price nobody questions.

**In SQL, not pandas**, for a reason about cost rather than taste: the store can
answer it with a binary search per decision into a physically time-ordered column,
and pandas has to hold both sides in memory to do the same thing. That is the whole
finding of `bench/RESULTS.md` restated — in-process DuckDB needed 2.5 GB where
QuestDB needed +297 MB — and it is why the DataFrame layer here never sees more
than the answer.

**`ASOF`, not `LT`, and the boundary was checked against a live server.** QuestDB
has both and they differ by exactly one case: `ASOF JOIN` takes the last row at or
before, `LT JOIN` the last row strictly before. Against trades at 10:00:00 and
10:00:10 and a decision at exactly 10:00:10, ASOF returned the 10:00:10 print and
LT returned the 10:00:00 one. Neither leaks the future, so this is not a
correctness trap in the way nearest-matching is — but LT would discard a trade that
happened *at* the decision instant, which on a platform where decision times are
generated from trade times is not a rare case. The platform's rule is at or before,
so this is the one line where a look-ahead bug would live, and what each variant
returns is written down rather than assumed. The other QuestDB specifics of this
query — the degenerate join key, the staged timestamps and the absent `TOLERANCE` —
are in section 3.2b.

**The same rule as the model layer**, enforced twice in different languages:
trailing volatility admits only bars that *end* at or before the decision time.

The obvious way to write that is `bucket < query_ts`, and it is wrong in a way
that is almost impossible to see, because a bar is stamped with the start of the
interval it covers. A decision at 10:50:17.5 then admits the bar stamped
10:50:00, which covers 10:50:00 to 10:51:00 and whose close is therefore built
from trades taken after the decision. Measured on this repo's own demo data, that
one bar moved realised volatility by a factor of 52 when a large print landed
twelve seconds past the decision time. The condition is now full containment:
`bucket >= query_ts - window` and `bucket + 1 minute <= query_ts`.

It also hid from the test suite, which is the part worth taking seriously. A
query timestamp falling exactly on a bar boundary is the single case where the
two versions agree, and that is what the original test used. A feature that leaks
the future only when the decision time is off the minute is a feature that leaks
the future in production and passes in CI.

**The same discipline on the bar query itself.** `DuckDBStore.bars` used to filter
raw trade time rather than the bucket, so a range starting at 10:00:30 returned a
10:00 bar built from only the trades inside the range — half the volume, a wrong
open, labelled as a whole bar with nothing marking it. The other backends filtered
on the bucket and dropped that bar, so two backends disagreed with each other about
the same data — which is how it was found. Both now filter on the bucket, and the
QuestDB version has a second trap of its own in section 3.2b: `SAMPLE BY ... FROM`
moves the grid instead of narrowing it. A partial bar is worse than a
missing one, because the missing one is visible.

### 3.5 Contracts as data, not code

**Because** what a dataset promises should be readable by somebody who does not
read Python, diffable in a pull request, and enforceable before a row is written.
Each dataset has a JSON contract with types, ranges, keys, cross-field rules and
lineage.

**Consequence worth stating:** a missing required column stops the load entirely,
because that is a provider changing their format, while a bad row is quarantined
with a reason and the rest still publishes. Those are different failures and they
deserve different responses.

**Two limits, counting different things.** `max_null_fraction` caps the rows lost
because a required value was absent or would not coerce, which is the shape of a
mangled or wrongly-delimited file; `max_reject_fraction` is the blunter cap on
rows lost for any reason at all. They were one key for a while, named for nulls
and counting every rejection, which meant a file failing a cross-field rule
tripped a limit about missing values. Worse, a breach was recorded and then
ignored: the loader published anyway. A threshold the contract declares and the
loader does not enforce is worse than no threshold, because it reads as a
guarantee in review and delivers nothing at 6am. A breach now blocks.

**Alternatives rejected:** validation scattered through loader code, which drifts
and is invisible to anyone reviewing a change; Great Expectations, which is good
and is more machinery than two datasets justify; validating after load, which means
the bad data was already visible to research. Pandera is the fairer comparison
than Great Expectations and is argued properly in section 3.14.

### 3.6 Sources defined in YAML

**Because** the answer to "how fast can you onboard a new dataset?" should be
"one config file", and it should be demonstrable live. Endpoint, symbols, retry
policy, batch size, partitioning, checks and SLA all live in one file per source.

**Alternatives rejected:** a Python module per source, which is a code review and a
deploy for every new instrument; a UI-driven tool, which puts the definition
somewhere git cannot see it.

### 3.7 Acquisition separated from storage

**Because** they fail differently and change for different reasons. Vendor quirks,
paging and retries belong to acquisition; batching, file layout and compression
belong to storage. Keeping them apart is what lets the same pipeline run against
more than one store — which is what made section 3.1's choice a module swap rather
than a rewrite — and what will let the tick path move to a capture process later
without touching the loaders.

### 3.8 Table design

A schema is not portable between these engines. Each one's grain enforcement is
different enough that copying one into the other produces a table that looks right
and deduplicates nothing, so `sql/02_questdb.sql` carries its own reasoning in full
rather than describing itself as a translation. The ClickHouse shape is recorded
below as the thing it is not, because that contrast is where most of the reasoning
came from and because section 3.2 keeps the door open to going back.

**On QuestDB: a designated timestamp and `DEDUP UPSERT KEYS`.** Rows are stored
physically ordered by the designated timestamp, which is what makes the as-of join
a binary search rather than a scan, and dedup is enforced *at commit* rather than
eventually. That is strictly better than waiting for a merge: there is no window in
which a replayed print sits in the table, so nothing on the read path needs the
equivalent of `FINAL` and the as-of join cannot match a superseded row. The read
path gets faster and more correct at once, which is not a trade-off that comes up
often. The cost is the hole in section 3.2b — the designated timestamp must be in
the key set, so a corrected trade timestamp lands beside the original.

`eod_bars` needs the *opposite* behaviour and the schema says so, because the
obvious thing is wrong: an exchange restates a settlement days later, and that
correction must not overwrite the row a backtest published last week. So
`ingested_at` is *in* the dedup key set — a correction carries a later ingest time,
which makes it a new row — and `_latest_per_grain` resolves them on read by keeping
the highest `ingested_at` per grain. Point-in-time history stays in the table; the
current answer is computed on the way out.

**On ClickHouse it was `ReplacingMergeTree`**, because feeds replay on reconnect and
the grain `(venue, symbol, trade_id)` makes a print unique, so replays collapse
rather than double-count. Deduplicating in application code is slower and wrong
under concurrent loads.

`ORDER BY (venue, symbol, trade_id)` **because** on a `ReplacingMergeTree` the
sorting key is not only the index, it is the deduplication key. The obvious
choice — filter columns then time, `(venue, symbol, ts, trade_id)` — reads better
and is wrong: `ts` is a column a vendor correction can move, so a resend with a
corrected timestamp sorts to a different position and becomes a second row rather
than replacing the first. The sort key is therefore the declared grain, and
`ingested_at` is the engine's version column so that on a collapse the newest
ingest wins rather than an arbitrary one.

Time pruning was bought back rather than given up: the partition key is
`toYYYYMM(ts)`, and a `minmax` skipping index on `ts` restores granule skipping
within a partition, which works well because trades arrive roughly in time order.
Both are cheap. Correctness is not.

**One caveat stated rather than hidden:** `ReplacingMergeTree` only collapses
rows within a partition, so a correction that moves `ts` across a month boundary
lands elsewhere and will never collapse. Partitioning on something a correction
cannot move removes the hole at the cost of read pruning; for this dataset a
cross-month `ts` correction is a manual-repair event and is listed as one.

Daily partitions on trades and yearly on end-of-day, **because** partitions should
match how data is dropped and aged, not how it is queried. On QuestDB the trades
partitioning also does read work: a decision timestamp becomes a partition lookup
followed by a binary search inside one time-ordered column.

Batched inserts, with the batch size in each source's config, **because** a column
store pays a fixed cost per write that does not shrink with the row count — QuestDB
commits a WAL transaction and then waits for the apply job, ClickHouse creates a
part per insert and merges it later — and the two sources here want very different
sizes. Asynchronous inserts were the ClickHouse half of that and were a client
setting applied per connection, not a per-source choice; the source YAML carried an
`async_insert` key for a while that nothing read, which is the kind of dead knob
that reads as a guarantee in review.

### 3.9 Bars, and the version of them that was wrong

**Because** a researcher asking for minute bars should never wait for a batch to
finish or read a half-built table. Where the bar is computed is then a property of
how the store enforces the grain, which is why QuestDB and ClickHouse land in
different places for the same reason.

**On QuestDB there is no bars table and no view.** `SAMPLE BY` aggregates the
time-ordered trades column at query time, and — this is the part that decides it —
`DEDUP` has already made a replayed print impossible to double-count, so the bar is
computed from one row per print with no merge to wait for. No stale-bar window,
nothing to refresh, no second copy of the data to keep consistent. What is given up
is a precomputed answer: a minute bar over a wide range costs a scan of the trades
in that range rather than a read of an aggregate, which on these read patterns is
the better trade. If it stops being one, the fix is a materialised view over the
same `SAMPLE BY` — not a return to insert-triggered aggregation, for the reason
below.

**On ClickHouse the table has to be maintained**, so `bars_1m` was an
`AggregatingMergeTree` maintained by the server itself, with coarser frequencies
rolled up from it.

**Alternatives rejected:** a scheduled aggregation job outside the database,
which adds a dependency, a window where the data is stale and a failure mode at
6am; computing bars in the client, which means every consumer reimplements them
slightly differently.

**What had to change, and it is the more interesting half.** The view was
originally insert-triggered, which is the textbook ClickHouse answer and is
wrong for this table. An insert-triggered materialised view is a trigger on the
*inserted block*: it never reads `mdp.trades`, only the rows that just arrived,
and it fires before `ReplacingMergeTree` has collapsed anything. So when the
feed reconnects and replays the last few thousand prints — the exact behaviour
the trades table is deduplicated for — those prints are aggregated a second time
into the same bucket. `volume`, `trades` and `notional` are then permanently
overstated, merges never repair it, and nothing in the platform reports it.
Open, high, low and close survive, because `argMin`, `argMax`, `min` and `max`
are idempotent; the three additive columns are not.

The answer is a **refreshable** materialised view over `trades FINAL`, which sees
one row per `(venue, symbol, trade_id)` however many times it was sent. Both
definitions were carried in the ClickHouse schema, one commented out, because the
trade-off is the thing worth reading:

| | Incremental | Refreshable (what shipped) |
| --- | --- | --- |
| Freshness | correct within milliseconds of the insert | up to one refresh interval behind |
| Replay | sums double-count, permanently | `FINAL` collapses the replay first |
| Cost | proportional to rows inserted | proportional to table size, every refresh |

Correct-and-lagging beats fresh-and-wrong for a number a desk trades on, so
refreshable was the default. Freshness is recovered where it is actually needed:
the as-of path read `mdp.trades` directly and never went through the bars at all.
At real tick volume that view would get a `DEPENDS ON` and a partition-scoped
rebuild rather than a whole-table recompute.

Worth noticing what that comparison implies for the other backend: the whole class
of bug in it exists only because the grain is enforced eventually. On QuestDB, where
it is enforced at commit, neither column of the table above applies — which is the
read-path consequence of section 3.8 and not a separate opinion about bars.

All of this was found against a running server rather than by reading the file.
The same run found `toYYYY(trade_date)` in the end-of-day partition key, a function
that does not exist; the schema had been the production target for months and had
never been executed, so nothing had ever said so. That is the reason the QuestDB CI
job stands up the pinned server, applies `sql/02_questdb.sql` to it, drives the CLI
and the tests through it, and is not allowed to fail.

### 3.10 Reference data is point in time

**Because** a futures price series is meaningless without knowing which contract
it refers to on each day, and a backtest run in June must see June's map.

Three columns carry that. `trade_date` is the day the choice applies to;
`known_from` and `known_to` are the window during which we believed it. So one
symbol-day can have several rows, each true for a different stretch of calendar
time, and `get_continuous(as_of=X)` answers "which contract did we say was front
on that day, as of X" rather than "which contract would today's rule pick".

**That distinction is not cosmetic, and for a long time the code did not make
it.** `contract_reference` had no `trade_date` column at all, nothing ever read
the table back, and `as_of` was implemented by truncating the price history and
re-running the volume rule. Re-running the rule uses today's settlements, so a
correction that arrived last week silently changes which contract was front on a
day already sitting inside a published backtest — and the point-in-time claim was
false in precisely the place it was supposed to be true. Three changes fixed it:
the table gained `trade_date`, `pipeline.build_reference` versions rows instead
of deleting and rewriting them, and `serve.MarketData._roll_map` reads the stored
map back and hands it to `continuous_series`.

**Versioning means closing a belief, not replacing it.** When today's rule picks a
different front contract for a past day, the existing row gets
`known_to = yesterday` and a new row opens with `known_from = today`. Nothing is
deleted, so the run that used the old answer can still be reproduced.
`build_reference` returns `{opened, closed, unchanged}` rather than one number,
because "nothing changed" and "forty days were restated" are very different
mornings; a non-zero `closed` raises a warning alert, since somebody may hold a
backtest built on the previous map.

**A bug this caught:** the obvious rule, front contract equals highest volume
today, flips back and forth because daily volume does. On the demo series it
produces roughly sixty rolls a year. Forcing rolls forward only gives about
twelve, which is what a monthly contract can have. That class of error is
invisible for months and then shows up as a model that worked in research and not
in production.

**A second one, underneath it.** The forward-only rule originally made an
exception for a day on which the new front contract did not print, which
reintroduced exactly the reversal it existed to prevent: Apr, May, May, Apr, May.
The rule is now absolute, and the separate question — whether a contract printed
at all on a given day — is answered where it belongs, in `continuous_series`,
which drops such a day rather than carrying the previous close forward. A
carried-forward close is a flat day the exchange never published, and that
invented return goes straight into volatility, into every feature and into the
backtest. A calendar gap is true; a fabricated price is not.

The returned series carries `roll_adjusted` for the same reason: True where the
two contracts were spliced at a price both of them printed, False where no day
carries both and the roll could not be adjusted at all, null on every non-roll
row. An unadjusted roll leaves a real discontinuity in `adj_close`, and the
column exists so a caller finds that out by looking rather than by being
surprised.

### 3.11 A client library as the only interface

**Because** the storage layer will change and the researcher's code should not.
`get_bars`, `get_continuous`, `get_trades`, `catalog` — no SQL, no partition
knowledge, no roll logic in a notebook.

**Also because** it is where correctness gets enforced once rather than in fifteen
notebooks: the as-of rule, deduplication and the roll are inside the library.

### 3.12 Python only, no C++

**Because nothing in this platform's hot path is bound by the cost of interpreting
Python.** The as-of match, the bars and the point-in-time resolution all execute in
the store — that is what section 3.1 measured — and the Python around them handles
hundreds of rows and one HTTP round trip per batch. A compiled language would make
the part that is already microseconds faster, and the part that is seconds is a
network and a disk.

**Where it would genuinely matter**, stated so the boundary is clear rather than
implied: a tick capture process that must not drop a message while a consumer is
slow, and inserts at a rate where per-row native-protocol encoding dominates. What I
would do before reaching for C++ is what section 7 describes — separate capture from
parsing, batch larger, use the native protocol from the Python client, push work into
the database. And the honest personal half: Python is what I write, so a claim to
have built the compiled half of this would be found out in week one.

### 3.12a Models as recipes, and scenarios in isolation

**Because** a model that lives in a script is not comparable to itself six months
later. Inputs, features, target, validation and outputs are declared in versioned
YAML; the code reads the recipe rather than containing the model. Adding a variant
is config, and every run records which recipe version produced it.

**Scenarios do not share anything.** Each gets its own run id, parameters and
artifacts, so asking a new question never destroys the answer to the last one.
That is also the mechanism for shadowing a candidate model against the incumbent
before promoting it, which is the version of this that matters on a desk.

**A run records the data it saw**, not only its parameters: row count, date range
and a hash of the input series. Parameters alone do not reproduce a run, because
the data moves underneath them, and "why did this number change?" is then a
comparison rather than an argument.

**Scenarios at different horizons have to be purged before they can be
compared.** A target at horizon *h* is a forward return, so the label on the last
*h* training rows is computed from prices inside the test block. Without a gap,
the five-day scenario leaked five times as far into its test blocks as the
one-day scenario — and it leaked in the flattering direction, so the comparison
was partly measuring leakage and reporting it as a result. `walk_forward` now
takes the horizon and drops the last `h - 1` rows from the end of each training
window. The gap comes out of the end of training rather than the start of the
test block, because the test block has to stay contiguous or it is no longer a
simulation of trading it.

**What it produced here is worth the slide:** the eleven-feature scenario is
consistently the worst of the six, by roughly an order of magnitude against the
three-feature control, and it is the one with the fewest folds to learn from —
more parameters chasing less data. The overfitting lesson falls out of the
harness instead of being asserted, and a single train/test split would very
likely have made that scenario look like the winner. Exact figures are not quoted
here on purpose: the demo series is generated relative to the day it is run, so
any number in this file would be stale by the time it was read. `make recipe`
prints the table.

### 3.13 Ridge regression by normal equations

**Not because it avoids a dependency.** That was the old answer here and it is a
weak one: numpy is already a dependency, so the marginal cost of scikit-learn is one
line in `pyproject.toml`, and a hand-rolled solver is a liability unless it is
buying something specific.

**Because the property under review is the absence of look-ahead, and normal
equations make it auditable line by line.** `_fit` is nine lines: standardise on
the training columns, add an intercept, penalise everything but the intercept,
solve. A reviewer can see that the standardisation statistics come from the
training slice and not from the full frame, that the test block is never touched
before `_predict`, and that the packed coefficients carry the training `mu` and
`sigma` forward rather than recomputing them on test data. `sklearn.fit()` does
all of that correctly and puts it behind a call, which is fine in production and
useless when the question under review is whether the validation is honest. The one thing
this repo is claiming is exactly the thing a library call would hide.

**The model is still not the point**, and a gradient-boosted model would produce
better numbers, no more insight, and a much harder honesty conversation. On a
desk, where nobody is auditing the fit and the evaluation harness is the shared
artefact, scikit-learn is the right call and this file is twenty lines to delete.

### 3.14 What was deliberately left out

| Not used | Why not, and when it would earn its place |
| --- | --- |
| Kafka or a message bus | Two sources, one consumer, no fan-out. It earns its place when several consumers need the same stream, or when capture and processing must be decoupled to survive a slow consumer |
| Flink or streaming processing | The same. With true tick volume and real-time features, this is the right shape: bus, stream processor with quality gates, table storage |
| Spark | Nothing here does not fit in memory, and a single-node column store plus Python answers these queries in milliseconds where a Spark job pays seconds of scheduling before it reads anything. I have run Spark on large asset-level datasets and would not introduce it without a reason |
| dbt | The transformation layer is a `SAMPLE BY` in one query. With twenty curated datasets and several analysts writing SQL, it earns its place quickly, and I have used it in production. SQLMesh is the stronger version of this argument and is below |
| Iceberg or Delta | Worth it when several engines read the same files, or when time travel over cheap storage matters. With one engine it is a layer with no consumer |
| A feature store | One model, six features. It solves a problem this does not have |

The list above is the infrastructure half. The other half is the Python
libraries that do, in a package somebody else maintains, several of the things
this repo does by hand. Those are the harder omissions to defend, because in four
of the seven cases below the honest answer is that the library is better and the
hand-rolled version is the thing that should go.

| Not used | What it solves | When it earns its place | What it costs today |
| --- | --- | --- | --- |
| **Pydantic / pydantic-settings** | Declarative parsing, coercion and validation of config and payloads | `config.py` should be Pydantic now, not later. It hand-rolls `${env:VAR}` resolution, a `redact()` walker over nested dicts and frozen dataclasses with no validation on them, which is `pydantic-settings` plus `BaseModel` reimplemented and slightly worse. Contracts are the opposite case and stay as they are | The config half costs real maintenance: every new key is another untyped `raw.get(...)`. The contract half costs nothing, and that is the deliberate part — see below |
| **Pandera** | Schema and statistical validation of DataFrames, declaratively | When a third dataset arrives and the two failure modes below stop being the whole story. It is also the fair comparison to make, rather than Great Expectations | The hand-rolled gate keeps one behaviour Pandera does not express cleanly: a missing column halts the load and publishes nothing, while a bad row is quarantined with a reason and the rest still publishes. Two different failures, two different responses, from one pass over the frame |
| **ODCS (Open Data Contract Standard)** | A published schema for what `config/contracts/*.json` already are | Whenever a contract has to be read by something that is not this repo — a catalogue, a vendor, another team's loader | Nothing, and it should be adopted. These contracts converged on roughly its shape independently: schema, grain, quality rules, lineage, version. Naming the standard is free and stops this being a bespoke format |
| **MLflow Tracking** | Run registry, parameters, metrics, artifacts, comparison UI | The moment a second person runs models here. `runs.py` plus `recipes.py` is MLflow rebuilt, and it is the one reimplementation in this repo with no good excuse | The genuine addition over a default MLflow setup is that a run records a hash of the *input series*, not only its parameters — `fingerprint()` in `recipes.py`. That is a custom tag in MLflow, so the right move is MLflow with that tag, not this |
| **Polars** | Faster, lazier DataFrames with a stricter API | If the DataFrame layer ever became the bottleneck | It cannot become the bottleneck here, and that is by construction rather than by luck. The heavy work is in SQL: the as-of match is an `ASOF JOIN` in the store and bars are aggregated there too, both so the DataFrame layer never sees a year of ticks. Swapping pandas for polars would speed up the part that handles hundreds of rows |
| **SQLMesh** | Versioned SQL transformations with column-level lineage | Alongside dbt, and for the same trigger: twenty curated datasets and several analysts writing SQL. Its column-level lineage answers "why did this number change?" — which is the question the run registry was built to answer from the other end | Nothing today, because the transformation layer is one `SAMPLE BY`. Worth naming because it is the stronger of the two: dbt would give model-level lineage, SQLMesh gives it per column |
| **OpenLineage / Marquez** | The standard for emitting and storing lineage events across datasets and jobs | Same trigger as the cross-dataset lineage gap in section 5b: when a derived dataset has consumers of its own. OpenLineage is the emission format, Marquez the reference store | Nothing, and it is the specific thing that gap should be filled with rather than a bespoke graph. It is an ecosystem standard rather than one orchestrator's feature — Airflow ships a first-party provider, dbt emits it, and the asset graph here already holds the edges an event would carry — so the cost is a client rather than a design |

Three of these deserve a straight answer rather than a table cell.

**Why pandas at the boundary, and matplotlib for the figures.** Both are chosen by
what happens at that layer rather than by habit. The DataFrame is the interface the
serving API hands a researcher, and it never carries more than an answer: the as-of
match, the bars and the point-in-time resolution all happen in the store, so the
frame is hundreds of rows and the cost of constructing it is dominated by the round
trip. Both store drivers return pandas natively, so choosing it means no
conversion on the way out; choosing anything else means one, on every call, to make
the part that is already fast slightly faster. Matplotlib is the same test applied
to the output: these figures are PNGs in a slide deck and in an executed notebook,
so they have to be static, deterministic and producible headlessly by a CI job. A
browser-rendering library would add a runtime in order to deliver interactivity a
PNG cannot carry.

**Why contracts stay JSON rather than becoming Pydantic models.** Because they
are read by people who do not read Python. A contract is the artefact a vendor
conversation points at and the thing a reviewer diffs in a pull request to see
that `settle` went from required to optional. The same JSON drives three things
at once: the quarantine reasons a researcher reads in the rejects file, the
schema-version gate that stops a load when a provider reshapes a dataset, and
the column list that decides what is loaded at all. A Pydantic model would
express the same rules in a language one audience can read. `contracts.py` does
have a `_COERCERS` dict that is a worse `TypeAdapter`, and that part could go
either way; the contract being data could not.

**Why Pandera is the comparison and Great Expectations is not.** Great
Expectations is easy to argue against — it brings a data context, a store and a
docs site for what is, here, two datasets. Pandera is the honest alternative:
decorator-driven, DataFrame-native, no infrastructure, and it would express most
of `contracts.py` in a third of the lines. What it does not express cleanly is
the split this platform is built around. A missing column and a bad row are not
two severities of the same event; one is a provider changing their format and
stops everything, the other is a Tuesday and gets quarantined with a reason while
the rest of the file publishes. Getting Pandera to produce both outcomes from one
pass, with per-row reasons written to a quarantine file, is more custom code than
the gate it replaces.

Saying no to these is as much a part of the design as the yeses. Each is a real
tool I would reach for under specific conditions, stated above — and for
Pydantic in `config.py`, ODCS, MLflow and OpenLineage, the condition has already
been met.

### 3.14a Packaging and tooling

`pyproject.toml` is the single source of truth, `uv.lock` pins it, and `uv sync
--frozen` is what CI and the Dockerfile both install with. `requirements.txt` is
still in the repo, but it is a generated export (`make lock` regenerates it),
kept so that somebody without uv can still run the quickstart. Editing it by
hand is how a project ends up with two dependency lists that disagree.

ruff and mypy run in CI and as a pre-commit hook, and both are **blocking**. They
were introduced against code that predated them, so there was a backlog — 171 ruff
findings and 5 mypy errors — and it was paid down in the same pass rather than
parked behind `continue-on-error`, because a gate that is red on the day it is added
does not get fixed, it gets ignored and then removed. The mypy configuration is
deliberately not strict: `ignore_missing_imports` is on and `disallow_untyped_defs`
is off, because the finding worth having is the `Optional` dereferenced without a
check — which is what all but one of those five were — not annotation coverage.

There is no `continue-on-error` anywhere in the workflow. There used to be one, on
the ClickHouse job, and it was the honest signal that the repository did not
maintain that path — which is part of why that path is gone (section 3.2). The
primary store now has a service container: the `questdb` job applies
`sql/02_questdb.sql` to the pinned server, drives the CLI through it and runs
`tests/test_questdb.py` against it, so the behaviour of the store the platform
actually depends on is asserted on every push rather than on a developer's machine.
The remaining gap is narrower and worth naming: the `store` fixture in
`tests/test_platform.py` constructs `DuckDBStore` directly, so `MDP_STORE` has no
effect on the rest of the suite, and making that fixture honour it is what would
make the whole suite mean something on that job.

---

## 4. How this maps to the role

Line by line against the job post.

| What the role asks for | Where it is in this project |
| --- | --- |
| Own the full data delivery lifecycle | `pipeline.run_source`: procurement notes in the contracts, acquisition, landing, validation, checks, load, scoring |
| Procurement and vendor discussions | A daily three-number score per vendor (`vendor_quality` in `vendor_quality.yml`, computed in `quality.score_vendor`, stored in `vendor_scores`), quarantined rows kept as the evidence, and licensing recorded in each contract's lineage. Note what this is *not*: there is no multi-source arbitration here, because there is one source per field — see below |
| Pipeline design and deployment with logging and alerting | Dagster assets with blocking checks and trading-day partitions, a single-slot writer pool as the concurrency guard, structured run history in the run registry, Prometheus metrics with tested alert rules, and blocked loads that produce no materialisation rather than a red task |
| Database insertion optimised for minimal delivery latency | Batched inserts sized per source and WAL-committed on QuestDB, with the loader waiting for the apply job so a write is readable; bars computed in the store rather than in a nightly job; event-driven ingestion rather than polling; and the read path chosen by measuring the query that matters (`bench/RESULTS.md`) |
| Internal applications for quant developers | `serve.MarketData`, the client library |
| Standardised, consistent outputs across datasets | One contract format, one grain declaration, one landing convention for two very different feeds |
| Python, Bash, SQL on Linux | All four, throughout |
| ClickHouse | Benchmarked as a first-class candidate on 20M trades (`bench/RESULTS.md`, still runnable) and implemented as a full backend before that: engines, sort key as dedup key, the refreshable materialised view and the insert-triggered one it replaced, TTL path, insert strategy. All of that reasoning is in sections 3.8 and 3.9; the implementation was removed rather than half-maintained, and section 3.2 states what would bring it back — it is still the better engine for the point-in-time half and the only one of the two with replication, TLS and RBAC outside an Enterprise licence |
| Git, SSH, Agile | Repo with tests; every pipeline step is an `mdp` subcommand, so the whole platform is drivable over SSH with no orchestrator installed — which is also the portability claim, and a test asserts it |
| Market, alternative and supporting datasets | Exchange end-of-day, venue trade prints, and the reference data that makes both usable |
| Leading a team of 4+ | Not demonstrable in a repo. What is demonstrable: contracts, tests and documented decisions are the things that let other people work on this without me |

**The golden-source gap, stated plainly**, because it is the one row above where
the platform does less than the job description asks for.

There is no arbitration between sources. There cannot be: arbitration needs at
least two sources for the same field, and this platform has one per field.

`config/vendor_quality.yml` carries no ranking of sources per field, because a
config file that declares a feature the code does not have is a claim rather than a
design, and it is one anybody can check in thirty seconds.

The trigger for building the real thing is specific: a second vendor for a
dataset that already has one. It needs a schema change first — a `source` column
on the fact tables, so a row can say where it came from — and then a resolver
that picks per field by weight and records which source won, because "why is
today's settle different from the exchange file" has to be answerable afterwards.

What is real is the scoring half: the freshness, completeness and accuracy numbers
behind the daily vendor score, the thresholds that decide green from amber, and the
quarantined rows that are the evidence in the conversation. Scoring one source is
not arbitrating between two, and the difference is worth saying out loud rather
than leaving for somebody to discover.

---

## 5. Failure modes this is built to survive

| What goes wrong | What happens | Why it was designed that way |
| --- | --- | --- |
| Vendor changes a column name | Load stops, nothing published, run fails | A format change is a conversation with a vendor, not a bad row |
| A few malformed rows | Quarantined with reasons, the rest publishes | One broken row should not stop a release |
| Feed replays after a reconnect | Duplicates collapse on the declared grain at commit, so no read ever sees an uncollapsed print and nothing needs the equivalent of `FINAL` | Replays are normal, not corruption |
| Source clock ahead of ours | Future-stamped rows rejected, and the cursor is capped at the requested window end so one skewed print cannot drag the high-water mark past it | A clock problem silently poisons every time-based query, and a cursor that overshoots leaves a gap nobody ever requests |
| Vendor file truncated | `expected_coverage` blocks: a cut-short transfer leaves its last day with fewer symbols than the rest of the file, and a delivery that lost a chunk in the middle skips trading days the calendar knows about | Truncation looks exactly like a quiet day, and publishing 80% of a day hides the gap from everyone downstream |
| Most of a file is unreadable | `max_null_fraction` blocks; a handful of bad rows still quarantines and publishes | Past the limit the contract declares, it stops being a few bad rows and starts being a different file |
| Source late | Freshness check fails, load blocked, run fails | Stale data presented as current is the expensive failure |
| Transform found to be wrong | Replay from the landing zone | Which is why raw is written before anything is interpreted |
| A restore is run against bad landing files | Validated first; the existing table is left untouched and the event is recorded as `fail` | A restore is the operation you reach for when things have already gone wrong, so it is the last one that should be able to make them worse |
| No contract spec covers the date | `notional` raises `LookupError` rather than assuming a lot size of 1.0 | A silent hundredfold understatement of a position, produced by the function whose job is to prevent exactly that |
| Two cycles overlap | The second waits for the single-slot writer pool, which is instance configuration and holds across runs rather than only inside one | Two writers on one partition is a bad morning, and on DuckDB the second one does not get a lock at all |
| "Why did this number change?" | Run registry: parameters, code version, data cut-off, input hash, metrics | The question always gets asked, usually months later |

### When a file does not arrive

The runbook entry the `MdpEodFileLate` alert points at.

**What the alert means.** The newest end-of-day bar held is more than a trading
day behind the delivery deadline for the trading day named in the
`trading_day` label, and that deadline has itself already passed by more than the
calendar's grace period. It is deliberately measured against the deadline rather
than against the wall clock, so it stays true over a weekend: a Friday file that
never arrives pages on Saturday, which an earlier version of this rule could not
do because it was gated on the exchange being open at the moment of evaluation.

**First, decide which of three things happened.**

1. `python -m mdp.cli monitor` asks the opposite question to every check in
   `quality.py` — what should be here and is not — per symbol and per trading
   day. A day with no rows at all is usually a delivery that never ran. A day
   missing one symbol of five is a delivery that ran and was incomplete, which
   is the more dangerous of the two because the dataset looks populated and
   every query over it silently returns a subset.
2. `python -m mdp.cli calendar` confirms the day was a trading day and when the
   file was due. If the exchange was shut, the alert is wrong and the calendar in
   `config/calendars/mcx.yml` needs the holiday adding.
3. `SELECT * FROM quality_events WHERE check_name IN ('expected_arrival',
   'publication_timeliness', 'delivery_latency', 'contract')
   ORDER BY event_at DESC` says whether the file arrived and was *refused*, and
   whether it arrived late. A blocked load is not a missing file, and the two
   have opposite responses: a missing file is a vendor conversation, a blocked
   load is ours.

**If the file arrived and was blocked**, the quarantine directory holds the
rejected rows with a `_reject_reason` column. Fix or waive, then re-run the
source. Nothing downstream should publish in the meantime, which is what the
blocked load already enforces.

**If the file never arrived**, fetch it directly:
`python -m mdp.cli fetch --source mcx_bhavcopy` for today, or
`python -m mdp.cli backfill --start ... --end ...` for a range. Both take the
same path as the daily run, deliberately. If the exchange endpoint is
unreachable, a file dropped into `data/incoming/mcx_bhavcopy/` is ingested by
the watcher on arrival and is the same code path again.

**Do not** clear the alert by widening the freshness SLA or passing `--force`
around the gate. A check that people learn to route around is decoration, and
the gap stays invisible to the desk either way.

---

### 3.9a The parts people usually leave out

Each of these exists because of a specific way platforms fail, not for
completeness.

**Entitlements, enforced at the serving layer.** Market data arrives with a
licence naming who and what it may be used for. "Everyone internal" is a breach
waiting for an auditor. The serving layer is the only door every consumer goes
through, so the policy lives there, as config rather than code, and a denial is
logged as loudly as an allow, because the interesting audit question is usually
who tried. An unknown dataset is denied rather than allowed: the dataset nobody
has thought about the licence for is exactly the one to stop.

**Including the platform's own operational record**, which is the part that was
missing. `catalog`, `quality`, `access_log` and `vendor_scores` went through no
entitlement check at all, so the access log — the table that says who read the
licensed feed and when — was readable by anyone who could construct a
`MarketData`. Those four are now a dataset in their own right,
`platform_metadata`, declared in `config/entitlements.yml` alongside the market
data. The catalogue is additionally filtered per dataset rather than gated as a
whole: a consumer sees what it may read and no more, because a catalogue that
lists what you cannot have is an index of things to go and ask for, which is not
what an entitlement boundary is supposed to produce.

**Contract specifications, versioned.** A lot size that changed at the start of
2020 means every P&L and backtest spanning the change is wrong by a constant
factor, and constant factors are the hardest errors to see because the shape of
the curve still looks right. Same price, different notional: ₹6,200 is ₹620,000 a
lot today and ₹310,000 in 2017. The CRUDEOIL history in
`config/contract_specs.yml` is an illustrative change rather than the real spec
history, and the file says so — the mechanism is the point, and shipping a wrong
lot size that looked authoritative would be the exact error it exists to prevent.

**Schema versions as a decision.** A contract version change stops the load until
a person accepts it, and their acceptance is recorded. A provider reshaping a
dataset overnight should not quietly rewrite what consumers believe they are
reading.

**A restore that is actually exercised.** `restore` rebuilds the database from the
raw files, through the same contract gate, and a test asserts that every row in
the landing zone is accounted for — restored or rejected, none quietly lost. A
restore path that skips validation would be an efficient way to reinstate the bad
data you were removing, and a backup nobody restores is a hope.

**And it validates before it truncates**, which it did not always do. In the
other order, a landing file that failed the contract at schema level produced an
empty `clean`: the table was emptied and then nothing was put back. The quality
event recorded for it said `pass`, so the operational record — the one the
monitor and the metrics endpoint read — reported a successful rebuild of a table
that no longer had anything in it. A restore is the operation you reach for when
things have already gone wrong. It is the last one that should be capable of
making them worse, and a refusal is now recorded as `fail` with the table left
untouched.

**Contract specifications that refuse to guess.** `notional` raises a
`LookupError` when no spec version covers the date. It used to fall back to a lot
size of 1.0, which is the exact error `get_specs` warns about in its own
docstring, only worse: a silent hundredfold understatement of a position,
produced by the function whose entire job is to prevent it. An unknown symbol or
a date before the spec history begins is a question, not a number.

**Duplicate resolution that refuses to guess either.** `_latest_per_grain` keeps
the most recently ingested row per grain, which means it needs an ingest time to
be right. The two stores spelled the column differently, so the sort never ran on
DuckDB and the two backends resolved the same correction in opposite directions;
underneath that, DuckDB's insert filled missing columns with explicit `NULL`,
which overrides a column `DEFAULT`, so `ingested_at` was null on every row ever
loaded. A default that never fires is worse than no default, because the schema
says it is there. Both are fixed, and when the column is absent or entirely null
the function raises rather than silently keeping whichever row the query happened
to return last — which is how a correction loses to the row it was correcting.

**Secrets from the environment.** `${env:VAR}` in config, resolved at load,
redacted in anything printed, and a missing variable fails at startup rather than
as a confusing 401 at 6am.

**Retention with an exception.** Raw files age out on a policy; quarantine never
does, because rejected rows are the evidence in a vendor conversation and those
conversations are slow.

**Metrics a machine can scrape.** A status table a person reads is not monitoring.
Row counts, data age, check outcomes, vendor scores, cursor lag and calendar state
on a Prometheus endpoint, so the graph exists before anyone needs it and an alert
can fire on a trend rather than on one bad morning.

### 3.15 Two output surfaces, and nothing in between

**Because they answer different questions for different people.** Operational
health is a time series that an on-call engineer watches; research output is a
narrative a researcher reads once and then argues with. Putting both on one page
produces something nobody owns, which is the usual fate of an internal dashboard.

**Operations goes to Prometheus, Alertmanager and Grafana**, and three properties
of that stack decide it.

**The pull model means the exporter is stateless and the monitoring outlives the
application.** The platform's job ends at serving numbers on an endpoint: no
connection to hold, no queue to drain, no delivery state to lose on a restart, and
nothing to reconfigure when a second scraper appears. Prometheus decides when to
look, so the monitoring keeps working when the pipeline is the thing that has died —
which is the only moment it has to. A push-based pipeline metric, by contrast, stops
arriving precisely when the process stops, and "no data" is indistinguishable from
"nothing to report" unless something else is watching.

**PromQL's `rate` and `increase` over counters are built for alerting on a trend
rather than on a sample.** `increase(mdp_checks_total{status="fail"}[15m]) > 0` says
"a check failed at some point in the last quarter of an hour", which is a different
statement from "this one scrape looked bad" — and
`increase(mdp_checks_total{status="warn"}[1h]) > 5 for: 30m` says "warnings are
accumulating and have not stopped", which no single sample can say at all. Every rule
in `monitoring/rules/mdp.yml` is written in one of those two forms. Reproducing them
over a status table means reimplementing range vectors, badly, in the pipeline.

**Alertmanager separates detection from routing.** Grouping, silencing, inhibition
and the routing tree are the parts that decide whether alerts keep being read at all,
and keeping them out of the pipeline is why `alerts.py` can be small. The platform
decides severity and a stable `alertname`; what to do with an alert at 3am is
configuration in version control.

A platform that ships its own monitoring UI also asks a team to watch two screens
during an incident, and the second is always the one nobody opens. So the platform's
contribution is honest numbers on an endpoint; deciding what is bad belongs in alert
rules under version control, and drawing it belongs in Grafana.

Three decisions inside that are worth defending:

- **The exporter runs with the pipeline, not in the monitoring stack.** If they
  shared a lifecycle, "the platform is down" and "the monitoring is down" would
  be one event, and the alert you most need would be the one you cannot get.
- **Each scrape opens and closes its own read-only connection.** A long-lived
  reader would fight the daily load for the DuckDB file lock. The thing that
  watches the pipeline must never be why the pipeline stalled.
- **A failed scrape serves the last good payload plus a failure counter and a
  last-success timestamp.** "The endpoint is down" and "the endpoint is up but
  the numbers are frozen" are different incidents that get confused constantly,
  and they have their own alerts — `MdpExporterDown` on `up == 0`,
  `MdpExporterCannotReadStore` on the last-success timestamp going stale.
- **One probe query sits outside the per-section error handling.** Every section
  of the scrape swallows its own exception, which is right for a table that does
  not exist yet on a fresh install: one absent dataset should not blank the
  scrape. But those same handlers turn "the database is unreachable" into a
  clean, well-formed scrape containing no metrics at all — and an absent metric
  fires no alert. A monitoring endpoint that reports health by saying nothing is
  the worst possible failure mode, so `SELECT 1` runs before any of them and is
  allowed to raise.

**Naming a metric for the question it answers.** `mdp_cursor_lag_seconds` used to
be one number doing two jobs. It computed how far behind the *data* was — now
minus the timestamp of the furthest row loaded — which climbs on its own through
a quiet market with a perfectly healthy poller. The alert on it was written to
mean the other thing: how long since the cursor *moved*, which only climbs when
the pipeline has stopped. So it paged for a quiet market and stayed silent for a
dead poller. It is now two metrics, `mdp_cursor_position_age_seconds` and
`mdp_cursor_idle_seconds`, and `MdpTickFeedStalled` reads the second.

**Alerts arrive from two directions on purpose.** Prometheus rules are right for
trends and thresholds: a vendor score drifting below 0.8 for an hour, a cursor
that has not advanced, a file now past its deadline. They are the wrong shape for
a discrete event the pipeline observed and the metrics cannot reconstruct — this
load was blocked because `settle` went missing, these 412 rows failed a
cross-field rule, the front-month map was restated for forty symbol-days.
Exporting those as a gauge and re-deriving them in PromQL would be a worse copy
of something already known precisely, at the moment it happened, with the detail
attached.

So the pipeline pushes them, and **both paths end at Alertmanager** rather than
at their own destinations. That is the decision worth defending. Posting straight
to Slack from the pipeline works right up until the afternoon somebody needs to
mute a known-bad vendor and discovers there is nothing to mute: no grouping, so a
bad morning is one notification per symbol; no silences, so the only way to stop
the noise is to comment out the call; no inhibition, so the root cause and its
twenty consequences all page separately; and two rotations to configure instead
of one. Alertmanager owns grouping, silencing, inhibition and routing, and it
owns them for both paths at once. `alerts.py` is deliberately small because of
it — a severity, a stable `alertname` so grouping does not split when somebody
improves the wording, and the low-cardinality context promoted to labels. The
destination is a URL change away from PagerDuty or OpsGenie; the decisions about
what is worth waking somebody for stay in version control next to the scrape
config.

**Every staleness rule is gated on the trading calendar** — though not always by
gating on it directly. Wall-clock freshness is wrong for end-of-day data: on
Sunday the file is not late, it is not due. The obvious implementation, gating
the rule on "is the exchange open right now", is wrong in the other direction: a
Friday file that never arrives then cannot page until Monday night, by which
point the desk has traded a whole session on stale data. `MdpEodFileLate`
therefore measures lateness against the deadline rather than against now, so the
wall clock cancels out of the expression entirely and the rule needs no gate to
stay quiet at the weekend. Both bugs in that rule were valid PromQL doing the
wrong thing, which is why `monitoring/tests/mdp_test.yml` exists and CI runs
`promtool test rules` — parsing is not behaviour.

**Research goes to a notebook**, and it earns its place on the shape of the work
rather than on being the usual surface: this is a sequence of questions where each
answer decides the next one, so the call, the number it produced and the reasoning
about it belong in one artefact that can be re-run from the top. It is also the only
consumer of the serving API that is not a test, which makes it the thing that catches
an interface that is technically correct and unpleasant to use. It is executed on
render rather than committed with saved outputs, because a notebook whose stored
output disagrees with its code is worse than none. Every *number* in it comes through
`MarketData`: no file reads,
no SQL, no knowledge of where the parquet lives. A test enforces the letter of
that — it greps the committed cells for `read_parquet`, `SELECT `,
`store.query(`, `duckdb.connect` and `open(` — and it should be read as a
tripwire rather than as a proof. It cannot stop a researcher reaching the store
through a helper, and the notebook does legitimately import `mdp.model`,
`mdp.recipes` and `mdp.charts`, which are library code on top of the same API
rather than a way around it. A shortcut taken once in a notebook is a shortcut
copied into every notebook after it, so the cheap check is worth having; calling
it a guarantee would be overstating it.

**There is no BI layer, and that is the design.** The consumer of a market data
platform is code. The serving API is the product, the notebook is one caller, and
Grafana exists so somebody can confirm the platform is alive.

**Colour is a decision, not a preference.** Categorical hues in a fixed order so a
filter never repaints the survivors, status colours reserved for state and never
reused as a series, one measure per axis, and a palette chosen for separation
under the common colour-vision deficiencies rather than by eye. That last claim
is currently a property of the palette and not of the build: there is no
validator in the repo asserting it, and until there is, it is a design rule
somebody followed rather than one the tests hold to.

## 5b. What is still missing

An honest list, because "what would you add next?" is a better question than
anything already built, and a platform is never finished.

Shorter than it was, because most of the list got built. What remains, and why
each one is a deliberate wait rather than an oversight:

| Missing | Why it is not here | When it becomes urgent |
| --- | --- | --- |
| **Streaming ingestion** | The continuous source is polled every sixty seconds, which is right for a research platform and wrong for anything latency-sensitive | Real tick volume, or a consumer that needs sub-second data. Websocket capture first, a message bus when several consumers need the same stream |
| **Schema migration, not just detection** | A contract change is caught and must be accepted; there is no tooling to run old and new side by side | The first breaking change to a dataset with external consumers. Then: new version alongside the old, deprecation window, mapping table |
| **Storage tiering, and a TTL at all** | `sql/02_questdb.sql` declares no TTL, so raw ticks age out by a partition drop somebody runs rather than by the schema. The ClickHouse schema this repo used to carry had a delete TTL and is where that reasoning came from; moving cold partitions to cheap storage needs a server-side storage policy either way, which is why it cannot live in the DDL without breaking a fresh install | When tick history gets expensive, which is sooner than people expect |
| **Cross-dataset lineage** | Lineage is recorded per dataset in each contract, not as a graph. OpenLineage is the standard to emit it in and Marquez the reference store, and the asset graph already holds the edges an event would carry, so the cost is a client rather than a design | When a derived dataset has consumers of its own and "what breaks if I change this?" stops having an obvious answer |
| **Intraday session bucketing** | The calendar now parses the `session:` block it has always carried, and `session_bounds(day)` and `in_session(moment)` answer when a day's trading actually opened and closed — including the MCX evening session that runs past midnight, which is why `session_bounds` rolls the close forward when it is declared earlier than the open. Nothing downstream uses it yet: intraday bucketing is still plain UTC | The moment MCX intraday data arrives. Parsing the block was the small half, and it was done because a config key nothing reads is a claim nobody checked. The real work is deciding what a "session day" means for bucketing, for freshness and for the roll, and that decision needs the data in front of it |
| **Exactly-once under concurrent writers** | One writer per table is assumed, and DuckDB's exclusive file lock is expressed as a single-slot pool in both orchestrators rather than as a retry. On QuestDB the grain is deduplicated at commit, which makes a concurrent double-load harmless for `trades` but not for the tables whose dedup key carries an ingest time | Horizontal scaling of ingestion. Partition ownership per writer, or an idempotent upsert keyed on the grain |
| **Alert routing to a real destination** | Alertmanager routes to a local webhook that prints; the receivers are a URL change away from Slack, PagerDuty or OpsGenie | The first on-call rotation. The routing tree, grouping and inhibit rules are already the part that matters |
| **Recording rules and SLOs** | Alerts are on raw expressions; there is no error budget and no burn-rate alerting | When "is the platform healthy" needs an answer that survives a bad afternoon without paging. Then: recording rules for the expensive queries, an SLO on delivery-by-deadline, multi-window burn rate |
| **Scheduled research reports** | The notebook is run on demand | When somebody who will not open Jupyter needs last night's numbers. Then papermill on a schedule, rendered and mailed, which is the same notebook with parameters |

## 6. What I would do on day one at Minix

Not build this. This is a greenfield demonstration, and a running desk has
pipelines, a store and researchers who depend on both. I do not know what those
are, and the first item below is there because I do not.

1. **Map what exists** before changing anything: sources, pipelines, tables,
   consumers, and what breaks and how often. This includes the orchestrator and
   the store — whatever they are, they are constraints, not open questions.
2. **Make correctness measurable** — completeness, freshness and reconciliation
   against exchange files, per instrument. You cannot fix reliability you cannot
   see, and the three-number vendor score in this repo is a week of work to
   stand up against feeds that already exist.
3. **Add contracts to the feeds that break most**, one at a time, starting where
   a quarantine would have caught a real past incident. Not to every feed at
   once, because a gate that blocks a good load on its first day gets switched
   off.
4. **Leave the orchestrator alone.** Migrating orchestrators is a project that
   delivers researchers nothing. If the pipeline logic is already separable from
   it, that is a happy accident to preserve; if it is not, making it separable is
   worth more than moving it.
5. **Introduce a client library** only if researchers are already writing raw
   SQL against storage, because that is the coupling that makes every later
   change hard — and only once it is faster for them than what they do now.

## 7. What changes at 1 TB a day

The principles hold; the mechanics change in specific ways.

- **Capture and parse become separate processes.** Capture writes bytes and a
  timestamp and does nothing else, so a slow parser can never drop a message.
- **The contract gate moves off the critical path**, running between raw and
  served rather than before landing, so validation cost never delays capture.
- **Inserts get larger and rarer**, one writer per partition, with visible back
  pressure rather than implicit queueing.
- **Storage tiers**: recent data hot, older data aged to object storage on a TTL and
  queried in place.
- **A message bus earns its place**, because capture, validation and serving now
  have genuinely different rates.
- **The client library does not change**, which is the whole point of having one.

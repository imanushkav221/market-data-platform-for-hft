# Which store, measured rather than assumed

The as-of join is the query this platform exists to make correct: "the last trade
at or before each of N decision timestamps". Everything else it does — bars,
freshness, the roll — is either cheap or rare. So the store should be chosen on
that query, and the choice should be a measurement rather than a preference.

This directory is that measurement. `gen.py` writes the dataset once and every
engine loads the same bytes, so no engine gets an advantage from the generator and
the answers can be compared row for row.

## Setup

20,000,000 trades across 5 symbols over 31 days, and two sets of decision
timestamps — 5,000 and 50,000 — each deliberately offset 17.5 seconds past a
minute so none of them coincides with a bar edge or a trade.

Container: 2 cores, 7 GB RAM. QuestDB 8.2.1, ClickHouse 24.8.14, DuckDB 1.x.
Wall time is client-side and warm (first run discarded). Memory is the process
high-water mark (`VmHWM`) for the two servers, and `ru_maxrss` for in-process
DuckDB.

All three engines returned identical answers — 5,000 and 50,000 rows matched,
average price 57739.4238 and 57739.4744 — so this compares like with like.

## Results

### 20,000,000 trades

| Engine | 5,000 decisions | 50,000 decisions | Peak RSS attributable to the join |
| --- | --- | --- | --- |
| **QuestDB 8.2.1** | **0.029 s** | **0.081 s** | **+297 MB** |
| ClickHouse 24.8.14 | 1.06 s | 1.06 s | +839 MB |
| DuckDB | 1.94 s | 2.15 s | 2,502 MB total |

QuestDB is 36x faster than ClickHouse at 5,000 decisions and 13x at 50,000.

### 2,000,000 trades, the same query

| Engine | 5,000 decisions |
| --- | --- |
| **QuestDB 8.2.1** | **0.023 s** |
| ClickHouse 24.8.14 | 0.111 s |
| DuckDB | 0.21 s |

This second point is the one that makes the argument below empirical rather than
asserted. At 2M rows ClickHouse is 4.8x slower than QuestDB; at 20M it is 36x.
The query did not change and the number of decisions did not change. The only
thing that grew was the table, and the gap grew with it — which is what "one of
these costs has a term in the table size" looks like when you measure it twice
instead of reasoning about it once.

## What the shape of the numbers says, which matters more than the ratio

**ClickHouse's time did not change when the work went up tenfold.** 1.06 s at
5,000 decisions, 1.06 s at 50,000. That is the signature of a cost that is not
about the query at all: ClickHouse builds a hash table over the entire right-hand
side, and as-of semantics fix which side that is, so the 20M-row trades table
goes into memory every time. The measured +839 MB is about 42 bytes a row, which
is what the join columns come to. It will scale with the table and not with the
question, and there is no spill-to-disk variant — `grace_hash` excludes ASOF.
Index-based narrowing for ASOF is [an open issue](https://github.com/ClickHouse/ClickHouse/issues/38444).

**QuestDB's time scaled with the question and its memory barely moved.** 2.8x the
time for 10x the decisions, and +2 MB of resident memory for those extra 45,000
lookups. That is a binary search into a physically time-ordered column store,
per decision, with the table never materialised.

The ratio at this size is interesting. The *slopes* are the reason to choose.
At 20M rows ClickHouse is slower but fine; the question is what happens at 2
billion, and the answer is that one of these two approaches has a term in it
that grows with the table.

**One asymmetry found on the same table, which is a cost of the winner rather than
an argument for it.** QuestDB's backward scan does not terminate when the join key
will never match: the same 5,000 decisions answered in 0.062 s for a symbol that
exists in the table and hit the server's 60-second timeout for one that does not,
where both other engines return a column of nulls immediately. So the winning
engine's fast path has a slow path attached to a typo, and `QuestDBStore.as_of`
probes the key before joining — 2 ms when it matches, about 100 ms when it does
not — for exactly that reason.

## What this does not measure, stated plainly

- **One machine, 2 cores, 20M rows.** ClickHouse is built to scale across cores
  and nodes and this container gives it neither. A 16-core box would narrow the
  time gap; it would not change the memory slope, which is the finding.
- **Only the as-of join.** ClickHouse is better at the other half of this
  platform: `argMax(..., ingested_at) GROUP BY grain` for point-in-time reads is
  cleaner and scales better than QuestDB's `LATEST ON` with several partition
  keys, which scans.
- **Nothing about operating either.** QuestDB's open-source build has no
  replication, no TLS and no role-based access control; all three are Enterprise.
  ClickHouse is Apache 2.0 with all of it included. For a platform serving
  licensed exchange data, that is a real cost on the QuestDB side and it is not
  in any of the numbers above. It is also the one finding here that could reverse
  the decision without any of these timings changing.
- **Versions move, and one of these has.** The ClickHouse figures were taken on
  24.8.14, which was already end of life when they were written down; the platform
  later pinned 26.8, the LTS its refreshable materialised view needed, before the
  ClickHouse backend was removed altogether (see `docs/ARCHITECTURE.md` section
  3.2 — the removal does not touch these numbers, because `run.sh` drives each
  engine with its own raw SQL over HTTP and never went through that backend). The
  two properties that produce the flat line — no spill-to-disk for ASOF and no index
  narrowing — were both open at the time of writing, so the shape of the result is
  expected to hold; what cannot be claimed is that it was re-measured on a current
  build, and that is a limit on this table rather than a footnote. QuestDB is pinned at 8.2.1, the
  version measured here and the version every behaviour in `src/mdp/storage.py` was
  checked against; 8.3 adds `TOLERANCE` to ASOF JOIN, so moving that pin changes the
  numbers this choice rests on and belongs in the same change as a re-run.

## Reproducing it

```bash
python gen.py 20000000 5000 data      # writes trades.parquet + decisions.parquet
python gen.py 20000000 50000 data     # rewrites decisions.parquet with the larger set
```

The generator is seeded, so the second call reproduces the same trades byte for
byte and only the decision set changes — which is what makes the two columns of the
table comparable.

Then bring up both servers — QuestDB from `docker-compose.yml`, ClickHouse with the
`docker run` line in the header of `run.sh`, since it is no longer part of the
platform — load `trades.parquet` into each, and run each engine's own as-of form.
For QuestDB that is the same SQL `QuestDBStore` issues, so what is timed is the
query the platform actually sends; ClickHouse is driven with raw SQL of the shape a
ClickHouse backend would send.

`run.sh` does all of it: generates, loads, warms, times three runs each, and
prints the peak resident memory of both servers. It checks that all three engines
returned the same matched count and the same average price *before* reporting any
timing, which is not decoration — the first version of the script did not, and two
of the three were quietly answering over an empty table because each engine
resolves file paths against its own root and reports a path it cannot reach as
something other than a path problem.

Three wrinkles worth recording, all of which cost time and none of which is
documented:

- QuestDB's `read_parquet()` rejects files with large row groups — a 1M-row group
  fails with "Parquet file is likely corrupted" while the same data at
  50,000-row groups loads fine. It also rejects Arrow `large_string`.
- **`read_parquet()` reports a file it cannot find with the same "likely
  corrupted" message**, which sends you to inspect the file rather than the path.
  Relative paths resolve against the server's `cairo.sql.copy.root`, not the
  client's working directory. The script probes for this and says so plainly.
- ClickHouse's `file()` has the same failure with a different shape: a path
  outside `user_files_path` inserts nothing and reports success, so the table is
  simply empty afterwards. The script pushes Parquet over HTTP instead and
  asserts the row count before timing anything.

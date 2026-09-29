.PHONY: dagster dagster-install dagster-check dagster-materialise dagster-backfill dagster-arrival dagster-ticks dagster-monitor demo init ingest reference bars continuous model status test clean questdb watch drop fetch backfill monitor calendar poll restore metrics retention await asof recipe charts notebook lab exporter monitoring monitoring-down monitoring-local monitoring-check alert-sink alert-test install lock lint typecheck check hooks image

# Kept for the non-uv path: `pip install -r requirements.txt` then `make demo`,
# which is what the README documents and what somebody without uv will do. Under
# `make install` the package is installed properly and this is redundant but
# harmless.
export PYTHONPATH := src

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
# pyproject.toml is the source of truth; uv.lock pins it. --frozen installs
# exactly what the lockfile says and never quietly re-resolves.
install:
	uv sync --frozen

# Re-resolve, then regenerate requirements.txt from the result. The two must be
# updated together: requirements.txt is a generated artifact, not an input, and
# editing it by hand is how a project ends up with two disagreeing dependency
# lists.
lock:
	uv lock
	uv export --no-hashes --no-emit-project --all-groups \
		--format requirements.txt > requirements.txt

lint:
	uv run ruff check .

typecheck:
	uv run mypy

# What CI runs, minus the containers.
check: lint typecheck test

# Run the fast checks before the commit exists rather than after the push.
hooks:
	uv tool run pre-commit install

# Build the application image. `docker compose --profile monitoring up -d` does
# this for you; this target is for building it on its own.
image:
	docker build -t mdp:local .

# The whole story in one command. Safe to run live: no network needed.
demo: clean init ingest reference bars asof continuous model status

init:
	python -m mdp.cli init

ingest:
	python -m mdp.cli ingest --synthetic

# Real sources instead of the generator.
ingest-live:
	python -m mdp.cli ingest --source binance_trades
	python -m mdp.cli ingest --source mcx_bhavcopy

reference:
	python -m mdp.cli reference

bars:
	python -m mdp.cli bars --symbol BTCUSDT --freq 5m --limit 8

continuous:
	python -m mdp.cli continuous --symbol CRUDEOIL --limit 8

model:
	python -m mdp.cli model --symbol CRUDEOIL

status:
	python -m mdp.cli status

# ---------------------------------------------------------------------------
# Orchestration: Dagster, and only Dagster
# ---------------------------------------------------------------------------
# One orchestrator. The repository used to carry three runners, which was a hedge
# rather than a decision - see docs/ARCHITECTURE.md 3.4. The portability claim that
# the other two were there to make is now made by a test instead: nothing under
# src/mdp imports an orchestrator, and every step below is also an `mdp` subcommand
# in the target list above, so the hand-run path and the orchestrated path cannot
# diverge without a test going red.
#
# Dagster goes in its own virtualenv, not the project one. It brings a large tree
# (grpc, sqlalchemy, starlette) and installing it into .venv re-resolves the
# lockfile for everybody who only wanted to run the tests - which is the same
# reason it is an optional dependency group rather than a `dev` one.
DAGSTER_VENV ?= .venv-dagster
DG          := $(DAGSTER_VENV)/bin
export DAGSTER_HOME ?= $(CURDIR)/.dagster

dagster-install:
	uv venv $(DAGSTER_VENV)
	uv pip install --python $(DAGSTER_VENV) "dagster>=1.12" "dagster-webserver>=1.12" \
	  -r requirements.txt

# The instance config is what enforces the single-writer pool, and Dagster only
# reads it from $$DAGSTER_HOME, so it is copied rather than pointed at. Not
# overwritten: a deployment that has edited its own instance config keeps it.
$(DAGSTER_HOME)/dagster.yaml:
	mkdir -p $(DAGSTER_HOME)
	cp flows/dagster.yaml $(DAGSTER_HOME)/dagster.yaml

# The UI on :3000, with the daemon that runs the schedules and backfills.
dagster: $(DAGSTER_HOME)/dagster.yaml
	$(DG)/dagster dev -f flows/definitions.py

# Does the code location load, and do the assets, checks, partitions, jobs and
# schedules resolve? No store, no data, no UI. This is the one to put in CI.
dagster-check:
	$(DG)/dagster definitions validate -f flows/definitions.py
	$(DG)/dagster asset list -f flows/definitions.py

# Materialise one trading day end to end, non-interactively: eod_bars for the
# most recently closed partition, then the roll map, the continuous series and the
# model runs. Overridable: `make dagster-materialise PARTITION=2026-09-23`.
PARTITION ?=
dagster-materialise:
	MDP_SYNTHETIC=1 MDP_STORE=duckdb $(DG)/python flows/definitions.py \
	  $(if $(PARTITION),--partition $(PARTITION),)

# Replay a range. 2026-09-10..2026-09-18 is six runs, not nine: the weekend and
# Ganesh Chaturthi are not partitions, so there is nothing to skip.
RANGE ?= 2026-09-10..2026-09-18
dagster-backfill:
	MDP_SYNTHETIC=1 MDP_STORE=duckdb $(DG)/python flows/definitions.py \
	  --partition-range $(RANGE) --assets eod_bars

# The arrival-window path: poll from the publication deadline and load the file
# when it lands. Run this OR dagster-materialise for a given day, never both.
dagster-arrival: $(DAGSTER_HOME)/dagster.yaml
	MDP_SYNTHETIC=1 MDP_STORE=duckdb \
	  $(DG)/dagster job execute -f flows/definitions.py -j mdp_eod_arrival

# One incremental pass over the tick feed, from the cursor.
dagster-ticks:
	MDP_SYNTHETIC=1 MDP_STORE=duckdb $(DG)/python flows/definitions.py --assets trades

# The checks that have something to say when nothing ran.
dagster-monitor: $(DAGSTER_HOME)/dagster.yaml
	MDP_SYNTHETIC=1 MDP_STORE=duckdb \
	  $(DG)/dagster job execute -f flows/definitions.py -j mdp_monitor

# Go and get the file, then load it. The whole loop, no hands.
fetch:
	python -m mdp.cli fetch --synthetic

# Replay a date range through the same path a scheduled run takes.
backfill:
	python -m mdp.cli backfill --start 2026-09-01 --end 2026-09-24 --synthetic

# Wait for the end-of-day file inside its publication window.
await:
	python -m mdp.cli await-file --source mcx_bhavcopy --synthetic --no-sleep

# Keep a continuous source current: each pass takes only what is new.
poll:
	python -m mdp.cli poll --source binance_trades --synthetic --passes 5 --every 5

# The tick path's consumer: as-of price and trailing volatility per decision time.
asof:
	python -m mdp.cli asof --symbol BTCUSDT --count 8 --vol-window 10

# Rebuild the database from the raw files. The backup claim, exercised.
restore:
	python -m mdp.cli restore --source mcx_bhavcopy

# Run the model recipe: every scenario isolated, then compared.
recipe:
	python -m mdp.cli recipe --recipe crudeoil_daily

# Figures for the deck, drawn from the run that just happened.
charts:
	python -m mdp.cli charts

# Research output: execute the notebook top to bottom and render it.
# Executed, not committed with stale outputs. A notebook whose saved output does
# not match its code is worse than no notebook.
notebook:
	jupyter nbconvert --to html --execute --ExecutePreprocessor.timeout=600 \
		--output-dir docs --output research notebooks/research.ipynb
	@echo "docs/research.html"

# The same notebook, interactively, which is how it is actually meant to be used.
lab:
	jupyter lab notebooks/research.ipynb

# ---------------------------------------------------------------------------
# Monitoring
# ---------------------------------------------------------------------------
# Serve /metrics where the pipeline runs. Prometheus scrapes this.
exporter:
	python -m mdp.cli exporter --port 9108

# Prometheus, Alertmanager and Grafana. Grafana lands on the provisioned
# dashboard at http://localhost:3000 with no login.
monitoring:
	docker compose --profile monitoring up -d
	@echo ""
	@echo "  Grafana       http://localhost:3000   (dashboard is the home page)"
	@echo "  Prometheus    http://localhost:9090/alerts"
	@echo "  Alertmanager  http://localhost:9093"
	@echo ""
	@echo "  Now run, in separate shells:  make exporter   and   make alert-sink"

monitoring-down:
	docker compose --profile monitoring down

# The same stack on a machine with no Docker: three binaries, reading a config
# rendered from monitoring/ rather than a second copy of it. Point the binaries
# at what this prints. `monitoring/` stays the only place a threshold, an
# interval or a panel is defined; scripts/monitoring_local.py changes addresses
# and nothing else, and tests/test_monitoring_local.py asserts exactly that.
monitoring-local:
	python scripts/monitoring_local.py $(ARGS)

# Validate the rules before Prometheus loads them. A rule with a typo in it is
# a rule that never fires, and nothing tells you.
#
# `check config` rather than `check rules`, so the rule_files glob in
# prometheus.yml is resolved the same way Prometheus will resolve it at startup
# — a rule file that exists but is not globbed is the other silent failure.
#
# Then the unit tests, which are the part that checks behaviour rather than
# syntax. Both bugs found in mdp.yml were valid PromQL that did the wrong thing.
monitoring-check:
	docker compose --profile monitoring run --rm --no-deps \
		--entrypoint promtool prometheus check config /etc/prometheus/prometheus.yml
	docker compose --profile monitoring run --rm --no-deps \
		--entrypoint amtool alertmanager check-config /etc/alertmanager/alertmanager.yml
	docker compose --profile monitoring run --rm --no-deps -w /etc/prometheus/tests \
		--entrypoint promtool prometheus test rules mdp_test.yml

# Stands in for Slack or PagerDuty: prints what Alertmanager routed.
alert-sink:
	python scripts/alert_sink.py

# Push a pipeline alert through the real path, to prove the wiring end to end.
alert-test:
	MDP_ALERTMANAGER_URL=http://localhost:9093 python -c "from mdp.alerts import Notifier; \
		Notifier().blocked_load('mcx_bhavcopy', ['required_columns'], \
		'column settle missing from the vendor file')"
	@echo "pushed; check the alert sink and http://localhost:9093"

# Numbers a scraper can graph.
metrics:
	python -m mdp.cli metrics

# Age raw files out of the landing zone. Dry run; add --apply to mean it.
retention:
	python -m mdp.cli retention --days 90

# What should be here and is not.
monitor:
	python -m mdp.cli monitor

# Which days the exchange is open, and when the file is due.
calendar:
	python -m mdp.cli calendar

# Event-driven path: react when a file lands instead of waiting for a schedule.
watch:
	python -m mdp.cli watch

# Drops a vendor-shaped file into the watched directory, for the live demo.
drop:
	python scripts/drop_sample.py

test:
	python -m pytest tests -q

# Bring up the primary store. The pipeline still defaults to DuckDB, so the
# store has to be named explicitly once it is running; see get_store.
questdb:
	docker compose up -d
	@echo "MDP_STORE=questdb python -m mdp.cli init"

clean:
	rm -rf data/landing data/quarantine data/runs data/mdp.duckdb

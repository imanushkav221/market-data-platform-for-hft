"""Command line for the demo.

    python -m mdp.cli init
    python -m mdp.cli ingest --source binance_trades --synthetic
    python -m mdp.cli ingest --source mcx_bhavcopy --synthetic
    python -m mdp.cli reference
    python -m mdp.cli bars --symbol BTCUSDT --freq 5m
    python -m mdp.cli continuous --symbol GOLD --as-of 2026-06-30
    python -m mdp.cli model --symbol GOLD
    python -m mdp.cli status
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import timedelta

import pandas as pd

from .alerts import Notifier
from .arrival import await_arrival
from .calendar import load_calendar
from .charts import build_all as build_charts
from .config import SourceConfig
from .fetch import fetch_incremental, fetch_range, fetch_to_drop
from .landing import apply_retention
from .metrics import render as render_metrics
from .model import make_features, walk_forward
from .monitor import check_arrivals, review_vendor_scores
from .pipeline import build_reference, restore_from_landing, run_source
from .recipes import Recipe, comparison_table, run_recipe
from .runs import ModelRun
from .serve import MarketData
from .storage import get_store
from .watch import watch


def _store(args):
    return get_store(args.store)


def cmd_init(args) -> int:
    store = _store(args)
    store.ensure_schema()
    print(f"schema ready on {store.__class__.__name__}")
    return 0


def cmd_ingest(args) -> int:
    store = _store(args)
    store.ensure_schema()
    names = [args.source] if args.source else [c.name for c in SourceConfig.all()]
    failed = False
    for name in names:
        cfg = SourceConfig.by_name(name)
        params = {}
        if args.symbols:
            params["symbols"] = args.symbols.split(",")
        if args.days:
            params["days"] = args.days
        summary = run_source(
            cfg, store, synthetic=args.synthetic,
            accept_schema_change=getattr(args, "accept_schema", False), **params,
        )
        print(f"\n{cfg.name}  [{summary['run_id']}]")
        print(
            f"  acquired {summary['rows_acquired']}, clean {summary['rows_clean']}, "
            f"quarantined {summary['rows_quarantined']}, loaded {summary['rows_loaded']}"
        )
        print(summary["report"].summary())
        print(f"  vendor score {summary['vendor_score']:.3f} ({summary['vendor_status']})")
        if summary["blocked_by"]:
            print(f"  NOT PUBLISHED, blocked by: {summary['blocked_by']}")
            failed = True
    return 1 if failed and args.strict else 0


def cmd_reference(args) -> int:
    store = _store(args)
    n = build_reference(store, method=args.method)
    print(f"front-month map rebuilt: {n} rows ({args.method} rule)")
    return 0


def cmd_bars(args) -> int:
    md = MarketData(kind=args.store)
    df = md.get_bars(args.symbol, args.start, args.end, freq=args.freq, venue=args.venue)
    print(df.head(args.limit).to_string(index=False) if len(df) else "no bars")
    print(f"\n{len(df)} bars")
    return 0


def cmd_continuous(args) -> int:
    md = MarketData(kind=args.store)
    df = md.get_continuous(args.symbol, method=args.method, adjust=args.adjust, as_of=args.as_of)
    if df.empty:
        print("no end-of-day data for that symbol")
        return 0
    rolls = int(df["is_roll"].sum())
    print(df.tail(args.limit).to_string(index=False))
    print(
        f"\n{len(df)} days, {rolls} rolls, adjustment={args.adjust}, "
        f"as_of={args.as_of or 'latest'}"
    )
    return 0


def cmd_model(args) -> int:
    md = MarketData(kind=args.store)
    prices = md.get_continuous(args.symbol, adjust="ratio", as_of=args.as_of)
    if prices.empty:
        print("no data: run ingest first")
        return 1
    feats = make_features(prices, horizon=args.horizon)
    run = ModelRun(
        model=f"ridge_h{args.horizon}",
        params={"symbol": args.symbol, "horizon": args.horizon, "alpha": args.alpha,
                "min_train": args.min_train, "test_size": args.test_size,
                "features": feats.attrs["feature_cols"], "adjust": "ratio"},
        data_through=prices["trade_date"].max(),
    )
    result = walk_forward(
        feats, min_train=args.min_train, test_size=args.test_size, alpha=args.alpha,
        horizon=getattr(args, "horizon", 1),
    )
    metrics = result.metrics()
    if not metrics:
        usable = len(feats.dropna(subset=feats.attrs["feature_cols"] + ["target"]))
        print(
            f"not enough history: {usable} usable rows after feature warm-up, "
            f"need at least {args.min_train + args.test_size}. "
            f"Ingest more days, or lower --min-train."
        )
        return 1
    run.finish(metrics)

    store = md.store
    store.ensure_schema()
    store.insert("model_runs", run.to_frame())
    out = run.save_artifacts(folds=result.frame(), predictions=result.predictions)

    print(f"run {run.run_id}  code={run.code}  data through {run.data_through.date()}")
    print(json.dumps(metrics, indent=2))
    print(f"\n{result.verdict()}")
    print(f"\nartifacts: {out}")
    return 0


def cmd_fetch(args) -> int:
    """Go and get today's file, and land it where the watcher will find it."""
    from datetime import date as _date

    store = _store(args)
    store.ensure_schema()
    sources = [SourceConfig.by_name(args.source)] if args.source else SourceConfig.all()
    for cfg in sources:
        when = _date.fromisoformat(args.date) if args.date else None
        result = fetch_to_drop(cfg, when, synthetic=args.synthetic, force=args.force)
        print(result.summary())
        if result.fetched and not args.no_ingest:
            delivery = watch([cfg], store, once=True)
            for d in delivery:
                print("  " + d.summary())
    return 0


def cmd_backfill(args) -> int:
    """Replay a date range through exactly the same path as the daily run."""
    from datetime import date as _date

    store = _store(args)
    store.ensure_schema()
    cfg = SourceConfig.by_name(args.source)
    results = fetch_range(
        cfg, _date.fromisoformat(args.start), _date.fromisoformat(args.end),
        synthetic=args.synthetic,
    )
    fetched = [r for r in results if r.fetched]
    skipped = [r for r in results if not r.fetched]
    reasons: dict[str, int] = {}
    for r in skipped:
        reasons[r.skipped_reason or "unknown"] = reasons.get(r.skipped_reason or "unknown", 0) + 1
    print(
        f"{len(results)} calendar days -> {len(fetched)} trading days fetched, "
        f"{len(skipped)} skipped"
    )
    for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
        print(f"  {count} x {reason}")

    if not args.no_ingest and fetched:
        deliveries = watch([cfg], store, once=True, mode="backfill")
        loaded = sum(d.rows_loaded for d in deliveries)
        blocked = [d for d in deliveries if not d.published]
        print(f"ingested {len(deliveries)} deliveries, {loaded} rows, {len(blocked)} blocked")
    return 0


def cmd_await(args) -> int:
    """Wait for a trading day's file inside its publication window."""
    from datetime import date as _date

    store = _store(args)
    store.ensure_schema()
    cfg = SourceConfig.by_name(args.source)
    day = _date.fromisoformat(args.date) if args.date else None

    result = await_arrival(
        cfg, store, day, synthetic=args.synthetic,
        poll_seconds=args.poll, max_wait_minutes=args.max_wait,
        max_attempts=args.max_attempts, sleep=not args.no_sleep,
    )
    print(result.summary())
    return 1 if result.status == "missing" and args.strict else 0


def cmd_poll(args) -> int:
    """Keep a continuous source up to date, on its configured interval.

    Each pass asks only for what has arrived since the last successful load.
    """
    import time as _time

    store = _store(args)
    store.ensure_schema()
    cfg = SourceConfig.by_name(args.source)
    schedule = cfg.acquire.get("schedule") or {}
    every = args.every or int(schedule.get("every_seconds", 60))

    print(f"polling {cfg.name} every {every}s. Ctrl-C to stop.\n")
    passes = 0
    try:
        while True:
            summary = fetch_incremental(cfg, store, synthetic=args.synthetic)
            start, end = summary["window"]
            since = summary["cursor_before"]
            print(
                f"{end:%H:%M:%S}  window {start:%H:%M:%S}-{end:%H:%M:%S} "
                f"({'from cursor' if since else 'cold start'}) -> "
                f"{summary['rows_loaded']} rows"
                + (f", cursor now {summary['cursor_at']:%H:%M:%S}"
                   if summary.get("cursor_at") else "")
                + (f"  BLOCKED {summary['blocked_by']}" if summary["blocked_by"] else "")
            )
            passes += 1
            if args.passes and passes >= args.passes:
                return 0
            _time.sleep(every)
    except KeyboardInterrupt:
        print("\nstopped")
        return 0


def cmd_monitor(args) -> int:
    """What should be here and is not."""
    store = _store(args)
    store.ensure_schema()
    notifier = Notifier()
    gaps = []
    for cfg in SourceConfig.all():
        gaps.extend(check_arrivals(cfg, store, notifier=notifier))
    degraded = review_vendor_scores(store, notifier=notifier)

    if not gaps:
        print("no missing deliveries")
    if len(degraded):
        print(f"\n{len(degraded)} source(s) below the amber threshold")
    return 1 if (gaps and args.strict) else 0


def cmd_calendar(args) -> int:
    from datetime import date as _date

    cal = load_calendar(args.name)
    start = _date.fromisoformat(args.start) if args.start else _date.today()
    end = _date.fromisoformat(args.end) if args.end else start + timedelta(days=13)
    print(f"{cal.name} ({cal.timezone}), file due {cal.deadline} + {cal.grace_minutes}m grace\n")
    day = start
    while day <= end:
        reason = cal.why_closed(day)
        print(f"  {day} {day:%a}  " + ("trading" if not reason else f"closed - {reason}"))
        day += timedelta(days=1)
    return 0


def cmd_watch(args) -> int:
    store = _store(args)
    sources = (
        [SourceConfig.by_name(args.source)] if args.source else SourceConfig.all()
    )
    drops = [
        (c.acquire.get("drop") or {}).get("dir", f"data/incoming/{c.name}")
        for c in sources
    ]
    print("watching:")
    for cfg, d in zip(sources, drops, strict=True):
        print(f"  {cfg.name:16s} {d}")
    print("drop a file in and it is ingested on arrival. Ctrl-C to stop.\n")

    def announce(delivery) -> None:
        print(delivery.summary())

    try:
        handled = watch(
            sources, store, poll_seconds=args.poll, once=args.once,
            on_delivery=announce,
        )
    except KeyboardInterrupt:
        print("\nstopped")
        return 0
    if args.once:
        print(f"{len(handled)} deliveries handled")
    return 0


def cmd_restore(args) -> int:
    """Rebuild the database from the raw files. A backup nobody restores is a hope."""
    store = _store(args)
    store.ensure_schema()
    cfg = SourceConfig.by_name(args.source)
    before = store.query(f"SELECT count(*) AS n FROM {cfg.load_cfg['table']}")["n"].iloc[0]
    result = restore_from_landing(cfg, store, truncate=not args.append)
    print(
        f"{cfg.name}: {before} rows before, {result['rows_in_landing']} rows in the "
        f"landing zone, {result['rows_restored']} restored, "
        f"{result.get('rows_rejected', 0)} rejected by the contract"
    )
    return 0


def cmd_retention(args) -> int:
    """Age raw files out of the landing zone. Dry run unless --apply."""
    result = apply_retention(args.source, days=args.days, apply=args.apply)
    verb = "deleted" if result["applied"] else "would delete"
    print(
        f"{verb} {result['files']} files older than {result['older_than_days']} days "
        f"({result['bytes'] / 1e6:.1f} MB). Quarantine is never touched."
    )
    return 0


def cmd_metrics(args) -> int:
    store = _store(args)
    text = render_metrics(store)
    if args.out:
        from pathlib import Path as _Path

        _Path(args.out).write_text(text, encoding="utf-8")
        print(f"wrote {args.out} ({len(text.splitlines())} lines)")
    else:
        print(text, end="")
    return 0


def cmd_exporter(args) -> int:
    """Serve /metrics for Prometheus to scrape.

    A factory rather than a store: each scrape gets its own short-lived read-only
    connection, so the exporter cannot hold a lock the daily load needs.
    """
    import os

    from .metrics import serve as serve_metrics

    kind = getattr(args, "store", None)
    # Resolve the store the same way get_store does before deciding which
    # keyword arguments it will accept. The previous version checked `kind`
    # alone, so with MDP_STORE=questdb and no --store flag it decided "duckdb",
    # passed read_only=True, and QuestDBStore rejected it. Every scrape then
    # failed and the exporter served its last good payload -- which, from a cold
    # start, is nothing at all. A metrics endpoint returning an empty body is
    # indistinguishable from a healthy platform with no data, which is the exact
    # failure the probe query in metrics.render exists to prevent.
    resolved = (kind or os.getenv("MDP_STORE") or "duckdb").lower()

    def factory():
        kwargs = {"read_only": True} if resolved == "duckdb" else {}
        return get_store(kind, **kwargs)

    serve_metrics(factory, host=args.host, port=args.port)
    return 0


def cmd_asof(args) -> int:
    """The tick path's consumer: what was true at a list of moments."""
    md = MarketData(kind=args.store)
    bars = md.get_bars(args.symbol, None, None, "1m", args.venue)
    if bars.empty:
        print("no tick data yet: run `make ingest` or `make poll` first")
        return 1

    # Decision moments that deliberately do not line up with any bar boundary,
    # because real signal timestamps never do.
    buckets = pd.to_datetime(bars["bucket"], utc=True)
    offset = pd.Timedelta(seconds=args.offset)
    times = [t + offset for t in buckets.iloc[-args.count:]]

    frame = md.research_frame(args.symbol, times, venue=args.venue,
                              vol_window_minutes=args.vol_window)
    columns = ["query_ts", "trade_ts", "price", "staleness_seconds",
               "realised_vol", "bars_in_window"]
    print(frame[columns].to_string(index=False))

    stale = int(frame["stale"].sum()) if "stale" in frame else 0
    print(
        f"\n{len(frame)} decision times, {stale} with no usable price "
        f"(older than the staleness bound). Every match is at or before its "
        f"query time, which is the whole point."
    )
    return 0


def cmd_recipe(args) -> int:
    """Run a model recipe, every scenario isolated, and compare them."""
    store = _store(args)
    store.ensure_schema()
    recipe = Recipe.by_name(args.recipe)
    print(f"{recipe.name} v{recipe.version}, owner {recipe.owner}")
    print(f"  {len(recipe.scenarios)} scenarios, input {recipe.input.get('symbol')} "
          f"({recipe.input.get('adjust')}-adjusted)\n")

    results = run_recipe(store=store, recipe=recipe,
                         scenarios=args.scenarios.split(",") if args.scenarios else None)
    table = comparison_table(results)
    print(table.to_string(index=False))

    data = results.attrs["data"]
    print(f"\ninput: {data['rows']} rows, {data['first_date']} to {data['last_date']}, "
          f"series {data['series_sha']}")
    best = table.iloc[0]
    print(f"\nbest scenario: {best['scenario']}")
    print(results[results['scenario'] == best['scenario']].iloc[0]['verdict'])
    return 0


def cmd_charts(args) -> int:
    store = _store(args)
    comparison = None
    if not args.no_model:
        try:
            comparison = comparison_table(
                run_recipe(store=store, recipe=Recipe.by_name(args.recipe))
            )
        except Exception as exc:  # charts should still be drawn without a model
            print(f"(skipping model charts: {exc})")
    made = build_charts(store, comparison=comparison)
    for name, path in made.items():
        print(f"  {name:10s} {path}")
    print(f"\n{len(made)} figures written")
    return 0


def cmd_status(args) -> int:
    md = MarketData(kind=args.store)
    print("CATALOG\n" + (md.catalog().to_string(index=False) or "empty"))
    q = md.quality(limit=args.limit)
    print("\nRECENT CHECKS\n" + (q.to_string(index=False) if len(q) else "none"))
    v = md.vendor_scores()
    print("\nVENDOR SCORES\n" + (v.to_string(index=False) if len(v) else "none"))
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="mdp", description="market data platform")
    p.add_argument("--store", default=None,
                   help="duckdb (default) or questdb")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init").set_defaults(func=cmd_init)

    ing = sub.add_parser("ingest")
    ing.add_argument("--source")
    ing.add_argument("--synthetic", action="store_true", help="run with no network")
    ing.add_argument("--symbols")
    ing.add_argument("--days", type=int)
    ing.add_argument("--strict", action="store_true", help="exit non-zero if a load is blocked")
    ing.add_argument("--accept-schema-change", dest="accept_schema", action="store_true",
                     help="acknowledge a contract version change and publish")
    ing.set_defaults(func=cmd_ingest)

    ref = sub.add_parser("reference")
    ref.add_argument("--method", default="volume", choices=["volume", "nearest"])
    ref.set_defaults(func=cmd_reference)

    bars = sub.add_parser("bars")
    bars.add_argument("--symbol", required=True)
    bars.add_argument("--start")
    bars.add_argument("--end")
    bars.add_argument("--freq", default="1m")
    bars.add_argument("--venue", default="binance")
    bars.add_argument("--limit", type=int, default=10)
    bars.set_defaults(func=cmd_bars)

    cont = sub.add_parser("continuous")
    cont.add_argument("--symbol", required=True)
    cont.add_argument("--method", default="volume", choices=["volume", "nearest"])
    cont.add_argument("--adjust", default="ratio", choices=["ratio", "difference", "none"])
    cont.add_argument("--as-of", dest="as_of")
    cont.add_argument("--limit", type=int, default=10)
    cont.set_defaults(func=cmd_continuous)

    mod = sub.add_parser("model")
    mod.add_argument("--symbol", default="CRUDEOIL")
    mod.add_argument("--horizon", type=int, default=1)
    mod.add_argument("--alpha", type=float, default=1.0)
    mod.add_argument("--min-train", dest="min_train", type=int, default=250)
    mod.add_argument("--test-size", dest="test_size", type=int, default=21)
    mod.add_argument("--as-of", dest="as_of")
    mod.set_defaults(func=cmd_model)

    f = sub.add_parser("fetch")
    f.add_argument("--source")
    f.add_argument("--date", help="trading day, default the last one")
    f.add_argument("--synthetic", action="store_true")
    f.add_argument("--force", action="store_true", help="fetch even on a non-trading day")
    f.add_argument("--no-ingest", dest="no_ingest", action="store_true")
    f.set_defaults(func=cmd_fetch)

    b = sub.add_parser("backfill")
    b.add_argument("--source", default="mcx_bhavcopy")
    b.add_argument("--start", required=True)
    b.add_argument("--end", required=True)
    b.add_argument("--synthetic", action="store_true")
    b.add_argument("--no-ingest", dest="no_ingest", action="store_true")
    b.set_defaults(func=cmd_backfill)

    aw = sub.add_parser("await-file")
    aw.add_argument("--source", default="mcx_bhavcopy")
    aw.add_argument("--date", help="trading day, default the last session")
    aw.add_argument("--poll", type=float, help="seconds between attempts")
    aw.add_argument("--max-wait", dest="max_wait", type=float,
                    help="minutes to keep trying after the deadline")
    aw.add_argument("--max-attempts", dest="max_attempts", type=int)
    aw.add_argument("--no-sleep", dest="no_sleep", action="store_true",
                    help="do not wait between attempts (for demos and tests)")
    aw.add_argument("--synthetic", action="store_true")
    aw.add_argument("--strict", action="store_true")
    aw.set_defaults(func=cmd_await)

    pl = sub.add_parser("poll")
    pl.add_argument("--source", default="binance_trades")
    pl.add_argument("--every", type=int, help="override the configured interval")
    pl.add_argument("--passes", type=int, help="stop after N passes")
    pl.add_argument("--synthetic", action="store_true")
    pl.set_defaults(func=cmd_poll)

    mon = sub.add_parser("monitor")
    mon.add_argument("--strict", action="store_true", help="exit non-zero if anything is missing")
    mon.set_defaults(func=cmd_monitor)

    cal = sub.add_parser("calendar")
    cal.add_argument("--name", default="mcx")
    cal.add_argument("--start")
    cal.add_argument("--end")
    cal.set_defaults(func=cmd_calendar)

    w = sub.add_parser("watch")
    w.add_argument("--source")
    w.add_argument("--poll", type=float, default=2.0, help="seconds between passes")
    w.add_argument("--once", action="store_true", help="one pass, then exit")
    w.set_defaults(func=cmd_watch)

    rc = sub.add_parser("recipe")
    rc.add_argument("--recipe", default="crudeoil_daily")
    rc.add_argument("--scenarios", help="comma-separated subset")
    rc.set_defaults(func=cmd_recipe)

    ch = sub.add_parser("charts")
    ch.add_argument("--recipe", default="crudeoil_daily")
    ch.add_argument("--no-model", dest="no_model", action="store_true")
    ch.set_defaults(func=cmd_charts)

    ao = sub.add_parser("asof")
    ao.add_argument("--symbol", default="BTCUSDT")
    ao.add_argument("--venue", default="binance")
    ao.add_argument("--count", type=int, default=10, help="how many decision times")
    ao.add_argument("--offset", type=float, default=17.5,
                    help="seconds past each minute, to land off the bar boundary")
    ao.add_argument("--vol-window", dest="vol_window", type=int, default=30)
    ao.set_defaults(func=cmd_asof)

    rs = sub.add_parser("restore")
    rs.add_argument("--source", default="mcx_bhavcopy")
    rs.add_argument("--append", action="store_true", help="do not clear the table first")
    rs.set_defaults(func=cmd_restore)

    rt = sub.add_parser("retention")
    rt.add_argument("--source", help="one dataset, default all")
    rt.add_argument("--days", type=int, default=90)
    rt.add_argument("--apply", action="store_true", help="actually delete")
    rt.set_defaults(func=cmd_retention)

    mt = sub.add_parser("metrics")
    mt.add_argument("--out", help="write to a file instead of stdout")
    mt.set_defaults(func=cmd_metrics)

    ex = sub.add_parser("exporter", help="serve /metrics for Prometheus")
    ex.add_argument("--host", default="0.0.0.0")
    ex.add_argument("--port", type=int, default=9108)
    ex.set_defaults(func=cmd_exporter)

    st = sub.add_parser("status")
    st.add_argument("--limit", type=int, default=10)
    st.set_defaults(func=cmd_status)

    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    pd.set_option("display.width", 160)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

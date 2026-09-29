"""A market data platform, from exchange file to quant desk.

Layout mirrors the pipeline:
    config.py     source configs and dataset contracts
    acquire.py    fetching, retries, and a synthetic source for offline runs
    contracts.py  the validation gate
    landing.py    raw and quarantine zones
    storage.py    QuestDB or DuckDB behind one interface
    quality.py    load checks and vendor scores
    reference.py  expiries, front-month map, continuous series
    serve.py      the client a quant developer imports
    model.py      features, walk-forward evaluation, honest baseline
    runs.py       run registry
    pipeline.py   the whole thing, in order
"""

__version__ = "0.1.0"

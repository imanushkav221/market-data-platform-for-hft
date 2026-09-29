# syntax=docker/dockerfile:1

# The platform itself, containerised. Until now this repo shipped a compose file
# for everything the platform *depends on* — QuestDB, Prometheus, Grafana —
# and nothing for the platform, which is a slightly awkward thing to hand to an
# interviewer. This builds it.
#
# Two stages: resolve and install into a virtualenv, then copy just that
# virtualenv and the source into a clean runtime image. The build toolchain, the
# uv cache and the dev dependency group stay in the builder and never reach the
# image that runs. Result is a few hundred MB rather than a gigabyte, and the
# thing running in production has no compiler in it.

# ---------------------------------------------------------------------------
# Stage 1: build the virtualenv
# ---------------------------------------------------------------------------
FROM python:3.13-slim-bookworm AS builder

# uv pinned to the version that generated uv.lock. An unpinned build tool is a
# build that is reproducible right up until the day it is not.
COPY --from=ghcr.io/astral-sh/uv:0.12.19 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies first, from the lockfile alone, so editing source code does not
# invalidate the layer that took ninety seconds to build. --locked fails rather
# than silently re-resolving: if pyproject.toml and uv.lock have drifted, the
# build should say so instead of quietly installing something else.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=README.md,target=README.md \
    uv sync --locked --no-install-project --no-dev

# Then the project itself.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY config ./config
COPY sql ./sql
COPY scripts ./scripts
COPY flows ./flows
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# ---------------------------------------------------------------------------
# Stage 2: runtime
# ---------------------------------------------------------------------------
FROM python:3.13-slim-bookworm AS runtime

# Non-root, with a fixed uid so a bind-mounted data directory has predictable
# ownership on the host rather than whatever the daemon picked this time.
RUN groupadd --system --gid 10001 mdp \
 && useradd --system --uid 10001 --gid mdp --create-home --home-dir /home/mdp mdp

WORKDIR /app
COPY --from=builder --chown=mdp:mdp /app /app

# The venv's bin on PATH is all the activation a container needs. No PYTHONPATH:
# mdp is installed as a package from src/, so `python -m mdp.cli` resolves the
# same way it would for anyone who pip-installed it.
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MDP_STORE=questdb

# The landing zone and the DuckDB file, if this image is ever run in laptop
# mode. Owned by mdp so a non-root process can actually write to it.
RUN mkdir -p /app/data && chown -R mdp:mdp /app/data
VOLUME ["/app/data"]

USER mdp

EXPOSE 9108

# stdlib only: this image has no curl and no wget, and adding one just to answer
# a health check is 15MB spent on nothing.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9108/metrics', timeout=4).status == 200 else 1)"]

# The metrics exporter is the default because it is the only long-running
# process in here. Everything else the CLI does is a command you pass instead:
#   docker compose run --rm mdp python -m mdp.cli status
CMD ["python", "-m", "mdp.cli", "exporter", "--port", "9108"]

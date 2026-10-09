"""
api/routes/pipeline_health.py

GET /health/pipeline — public pipeline-health probe for the external daily
health check (and any uptime monitor). No API key.

Answers one question: is every job running on schedule, and are fresh
scores being produced? Returns HTTP 200 when ``status`` is ``ok`` and 503
when it is ``degraded`` or ``down``.

Reads ONLY Redis (one MGET), never Postgres:
    pipeline:last_run:{job}         — written only after a SUCCESSFUL run
    pipeline:last_start:{job}       — written when a run starts
    pipeline:last_run_counts:{job}  — {tickers_ok, tickers_total} (per-ticker jobs)
    pipeline:last_tick              — end-of-tick summary from the scoring tick
(all written by pipeline/scheduler.py). A hung job stops advancing its
last_run, which is how a hang shows up here.

Status
------
    down     — scoring tick or newest score > 2 h old, or Redis unreachable
    degraded — any job past its limit; latest tick scored < 95% of the active
               universe; or > 20% of the latest tick lacks the narrative channel
    ok       — anything else
``reasons`` lists every failed condition in plain words.

The response carries only timestamps, counts and fixed reason strings —
never keys, hostnames, connection strings or exception text (those are
logged server-side only). Responses are cached in-process for 60 s; each
client IP is limited to 30 requests/minute in its own Redis namespace,
separate from the per-key /v1 limits.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from datetime import time as dtime

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from api.rate_limit import _INCR_WITH_TTL
from api.routes.demo_key import _client_ip
from pipeline.market_calendar import in_session, last_session_close
from scripts.db.redis import get_redis

_log = logging.getLogger(__name__)

router = APIRouter()

# ── Configuration (limits from the schedule in METHODOLOGY.md §2) ─────────────

#: Interval jobs: max minutes since the last success, (in session, outside
#: session). None = not expected outside the NYSE session.
INTERVAL_LIMITS_MIN: dict[str, tuple[int, int | None]] = {
    "scoring_tick": (45, 75),  # every 15 min in session / 30 min outside
    "narrative": (90, 90),  # :05/:35, runs 20-50 min
    "market": (45, None),  # every 15 min, session only
    "macro_intraday": (90, None),  # hourly, session only
    "influencer": (7 * 60, 7 * 60),  # every 6 h, runs ~40 min
    "macro_daily": (26 * 60, 26 * 60),  # daily 02:00
}

#: Once-per-session jobs: must have succeeded after the most recent session
#: close, checked from max(close + grace, UTC floor time on that day).
SESSION_JOB_DEADLINES: dict[str, tuple[int, dtime]] = {
    "market_eod": (60, dtime(21, 45)),  # scheduled 21:15 UTC
    "short_volume": (60, dtime(22, 30)),  # scheduled 21:30 UTC
}

DOWN_AFTER_MIN = 120  # scoring tick / newest score older than this → down
MIN_TICK_COVERAGE = 0.95  # tickers_scored / active_universe
MAX_SHARE_MISSING_NARRATIVE = 0.20
RESPONSE_CACHE_S = 60
IP_RATE_LIMIT_PER_MIN = 30

#: Response order of the jobs.
JOB_ORDER = (
    "scoring_tick",
    "narrative",
    "market",
    "market_eod",
    "short_volume",
    "influencer",
    "macro_daily",
    "macro_intraday",
)

#: Jobs that record per-ticker success counts.
COUNTED_JOBS = frozenset({"market", "market_eod", "influencer", "narrative", "short_volume"})

_LABELS = {
    "scoring_tick": "scoring tick",
    "narrative": "narrative job",
    "market": "market job",
    "market_eod": "end-of-day market job",
    "short_volume": "short volume job",
    "influencer": "influencer job",
    "macro_daily": "macro daily job",
    "macro_intraday": "macro intraday job",
}

TICK_SUMMARY_KEY = "pipeline:last_tick"  # mirrors pipeline.scheduler.TICK_SUMMARY_KEY

_cache: tuple[float, int, dict] | None = None  # (monotonic expiry, http status, body)


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _parse_ts(raw) -> datetime | None:
    if raw is None:
        return None
    try:
        dt = datetime.fromisoformat(raw.decode() if isinstance(raw, bytes) else raw)
    except (ValueError, TypeError, AttributeError):
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _parse_json(raw) -> dict | None:
    if raw is None:
        return None
    try:
        val = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return val if isinstance(val, dict) else None


def _int_or_none(val) -> int | None:
    return val if isinstance(val, int) and not isinstance(val, bool) else None


def _iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


def _minutes_since(dt: datetime | None, now: datetime) -> int | None:
    return None if dt is None else max(0, int((now - dt).total_seconds() // 60))


def _fmt_minutes(m: int) -> str:
    h, mm = divmod(int(m), 60)
    if h and mm:
        return f"{h}h {mm}m"
    return f"{h}h" if h else f"{mm}m"


def _session_deadline(close: datetime, grace_min: int, floor: dtime) -> datetime:
    floor_dt = datetime.combine(close.date(), floor, tzinfo=timezone.utc)
    return max(close + timedelta(minutes=grace_min), floor_dt)


def _redis_keys() -> list[str]:
    keys = [TICK_SUMMARY_KEY]
    for job in JOB_ORDER:
        keys += [f"pipeline:last_run:{job}", f"pipeline:last_start:{job}"]
        if job in COUNTED_JOBS:
            keys.append(f"pipeline:last_run_counts:{job}")
    return keys


# ── Evaluation ────────────────────────────────────────────────────────────────


def evaluate(values: dict[str, object], now: datetime) -> tuple[str, dict]:
    """Pure: Redis values (key → raw) + the clock → (status, response body)."""
    session = in_session(now)
    reasons: list[str] = []
    down = False
    jobs: dict[str, dict] = {}

    for job in JOB_ORDER:
        last_success = _parse_ts(values.get(f"pipeline:last_run:{job}"))
        last_start = _parse_ts(values.get(f"pipeline:last_start:{job}"))
        label = _LABELS[job]
        entry: dict = {"last_success": _iso(last_success), "last_start": _iso(last_start)}

        if job in INTERVAL_LIMITS_MIN:
            in_limit, out_limit = INTERVAL_LIMITS_MIN[job]
            limit = in_limit if session else out_limit
            age = _minutes_since(last_success, now)
            if limit is None:
                entry.update(
                    minutes_since=None, limit_minutes=in_limit, stale=False, note="market closed"
                )
            else:
                stale = age is None or age > limit
                entry.update(minutes_since=age, limit_minutes=limit, stale=stale)
                if stale:
                    reasons.append(
                        f"{label} has no recorded success"
                        if age is None
                        else f"{label} last succeeded {_fmt_minutes(age)} ago "
                        f"(limit {_fmt_minutes(limit)})"
                    )
            if job == "scoring_tick" and (age is None or age > DOWN_AFTER_MIN):
                down = True
        else:
            grace, floor = SESSION_JOB_DEADLINES[job]
            close = last_session_close(now)
            deadline = _session_deadline(close, grace, floor)
            if now < deadline:  # today's run not due yet → check the previous session
                close = last_session_close(close - timedelta(microseconds=1))
                deadline = _session_deadline(close, grace, floor)
            stale = last_success is None or last_success < close
            entry["stale"] = stale
            if stale:
                reasons.append(
                    f"{label} has not succeeded since the {close:%Y-%m-%d} session close "
                    f"(due by {deadline:%H:%M} UTC)"
                )

        if job in COUNTED_JOBS:
            counts = _parse_json(values.get(f"pipeline:last_run_counts:{job}")) or {}
            ok, total = (
                _int_or_none(counts.get("tickers_ok")),
                _int_or_none(counts.get("tickers_total")),
            )
            if ok is not None and total is not None:
                entry.update(tickers_ok=ok, tickers_total=total)

        jobs[job] = entry

    # ── Latest scoring tick ──────────────────────────────────────────────────
    latest_tick = None
    tick = _parse_json(values.get(TICK_SUMMARY_KEY))
    tick_at = _parse_ts(tick.get("at")) if tick else None
    if tick_at is None:
        down = True
        reasons.append("no scoring tick summary recorded")
    else:
        scored = _int_or_none(tick.get("tickers_scored")) or 0
        universe = _int_or_none(tick.get("active_universe")) or 0
        missing = _int_or_none(tick.get("missing_narrative")) or 0
        share_missing = round(missing / scored, 3) if scored else 0.0
        latest_tick = {
            "at": _iso(tick_at),
            "tickers_scored": scored,
            "active_universe": universe,
            "share_missing_narrative": share_missing,
        }
        tick_age = _minutes_since(tick_at, now)
        if tick_age > DOWN_AFTER_MIN:
            down = True
            reasons.append(
                f"newest score is {_fmt_minutes(tick_age)} old "
                f"(limit {_fmt_minutes(DOWN_AFTER_MIN)})"
            )
        if universe and scored < MIN_TICK_COVERAGE * universe:
            reasons.append(
                f"latest tick scored {scored} of {universe} active tickers "
                f"({scored / universe:.0%}, minimum {MIN_TICK_COVERAGE:.0%})"
            )
        if share_missing > MAX_SHARE_MISSING_NARRATIVE:
            reasons.append(
                f"{share_missing:.0%} of the latest tick is missing the narrative channel "
                f"(limit {MAX_SHARE_MISSING_NARRATIVE:.0%})"
            )

    status = "down" if down else ("degraded" if reasons else "ok")
    return status, {
        "status": status,
        "checked_at": _iso(now),
        "market_open": session,
        "jobs": jobs,
        "latest_tick": latest_tick,
        "reasons": reasons,
    }


def _unreachable_body(now: datetime) -> dict:
    return {
        "status": "down",
        "checked_at": _iso(now),
        "market_open": in_session(now),
        "jobs": {},
        "latest_tick": None,
        "reasons": ["state store unreachable"],
    }


async def _check_ip_rate_limit(request: Request) -> None:
    """30 req/min per client IP; own key namespace; fails open on Redis errors."""
    try:
        count = await get_redis().eval(
            _INCR_WITH_TTL, 1, f"rate:ip:health_pipeline:{_client_ip(request)}", 60
        )
    except Exception as exc:
        _log.debug("health_pipeline: IP rate limit skipped: %s", exc)
        return
    if isinstance(count, int) and count > IP_RATE_LIMIT_PER_MIN:
        raise HTTPException(
            status_code=429,
            detail={"error": "rate_limit_exceeded", "message": "Too many requests"},
        )


# ── Route ─────────────────────────────────────────────────────────────────────


@router.get("/health/pipeline")
async def health_pipeline(request: Request) -> JSONResponse:
    global _cache
    await _check_ip_rate_limit(request)

    if _cache is not None and time.monotonic() < _cache[0]:
        return JSONResponse(status_code=_cache[1], content=_cache[2])

    now = _now()
    keys = _redis_keys()
    try:
        raws = await get_redis().mget(keys)
    except Exception as exc:
        _log.warning("health_pipeline: state store read failed: %s", exc)
        raws = None

    if raws is None:
        status, body = "down", _unreachable_body(now)
    else:
        try:
            status, body = evaluate(dict(zip(keys, raws)), now)
        except Exception:
            _log.exception("health_pipeline: evaluation failed")
            status, body = "down", {**_unreachable_body(now), "reasons": ["health check failed"]}

    code = 200 if status == "ok" else 503
    _cache = (time.monotonic() + RESPONSE_CACHE_S, code, body)
    return JSONResponse(status_code=code, content=body)

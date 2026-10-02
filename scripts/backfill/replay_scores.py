"""
scripts/backfill/replay_scores.py

Score the empty 2026-09-17 13:42 → 2026-10-02 09:00 gap (scoring_tick was
hung) "as of" each past tick, using the live scoring code:

  - every scoring-path read is bounded by the tick time
    (scripts.db.queries.as_of.scoring_as_of), so nothing after t leaks in
  - pipeline.orchestrator.compute_scored_state does the scoring, with the
    previous replayed state kept in memory in place of Redis (EMA chain,
    per-layer fallback)
  - rows go to sentiment_history only, tagged replay_run (migration 014);
    Redis and price_snapshots are never touched

Run AFTER rebuild_narrative.py so the EMA chain is seeded from the rebuilt
Sep 17 rows. Resumable: completed (tick, ticker) rows are skipped and the
in-memory chain is re-seeded from the latest stored row before the first
incomplete tick.

Usage
-----
    python3 scripts/backfill/replay_scores.py --dry-run          # time one tick, no writes
    python3 scripts/backfill/replay_scores.py                    # full run, hourly ticks
    python3 scripts/backfill/replay_scores.py --run-id june-gap-2026-06 \
        --start 2026-06-23T15:00 --end 2026-07-03T05:30 \
        --ema-half-life 4 --no-surprise --no-positioning          # June gap, June-era settings
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

load_dotenv(override=True)


def _settings_parser() -> argparse.ArgumentParser:
    """Scoring settings that must be applied BEFORE the pipeline imports
    (ema.py reads the half-life at import time). Defaults match current prod."""
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--run-id", default="news-backfill-2026-10",
                   help="replay_run tag; also the resume key")
    p.add_argument("--ema-half-life", type=float, default=2.0,
                   help="EMA_HALF_LIFE_HOURS in force at the replayed time (4 before 2026-07-22)")
    p.add_argument("--surprise", action=argparse.BooleanOptionalAction, default=True,
                   help="ENABLE_NARRATIVE_SURPRISE at the replayed time")
    p.add_argument("--positioning", action=argparse.BooleanOptionalAction, default=True,
                   help="ENABLE_POSITIONING_FEATURES at the replayed time")
    return p


def apply_settings(argv: list[str]) -> argparse.Namespace:
    settings, _ = _settings_parser().parse_known_args(argv)
    os.environ["EMA_HALF_LIFE_HOURS"] = str(settings.ema_half_life)
    os.environ["ENABLE_NARRATIVE_SURPRISE"] = "1" if settings.surprise else "0"
    os.environ["ENABLE_POSITIONING_FEATURES"] = "1" if settings.positioning else "0"
    return settings


# Only a direct run changes the process env; importing (tests) leaves it alone.
_SETTINGS = (apply_settings(sys.argv[1:]) if __name__ == "__main__"
             else _settings_parser().parse_args([]))

from pipeline.orchestrator import compute_scored_state  # noqa: E402
from pipeline.persistence.pg_writer import persist_replay_row  # noqa: E402
from pipeline.scheduler import SCORE_TICKER_TIMEOUT_S  # noqa: E402
from scripts.db.connection import APP_COMMAND_TIMEOUT_S, close_pool, get_pool, init_pool  # noqa: E402
from scripts.db.queries.as_of import scoring_as_of  # noqa: E402
from scripts.db.queries.sentiment_history import get_baseline_scores  # noqa: E402
from scripts.db.queries.universe import get_active_tickers, get_ticker_sector_map  # noqa: E402

_log = logging.getLogger("replay_scores")

RUN_ID        = _SETTINGS.run_id
DEFAULT_START = datetime(2026, 9, 17, 14, 0, tzinfo=timezone.utc)
DEFAULT_END   = datetime(2026, 10, 2, 9, 0, tzinfo=timezone.utc)   # exclusive: live resumed 09:00

_LAYERS = ("market", "narrative", "influencer", "macro")


def ticks(start: datetime, end: datetime, step: timedelta) -> list[datetime]:
    out, t = [], start
    while t < end:
        out.append(t)
        t += step
    return out


def state_from_row(row: dict) -> dict:
    """
    Rebuild the last_state shape compute_scored_state reads (Redis layout)
    from a sentiment_history row: timestamp, smoothed score + EMA counter for
    the EMA chain, and per-layer value + as_of for the staleness fallback.
    """
    return {
        "ticker": row["ticker"],
        "timestamp": row["timestamp"],
        "composite_score": row["composite_score_smoothed"] or row["composite_score"],
        "composite_score_smoothed": row["composite_score_smoothed"],
        "ema_obs_count": row["ema_obs_count"] or 0,
        "sub_indices": {
            layer: ({"value": row[f"{layer}_index"], "n_signals": 1, "sources": []}
                    if row[f"{layer}_index"] is not None else None)
            for layer in _LAYERS
        },
        "freshness": {f"{layer}_as_of": row[f"{layer}_as_of"] for layer in _LAYERS},
    }


async def _seed_states(tickers: list[str], before: datetime) -> dict[str, dict]:
    """Latest stored row strictly before ``before`` for each ticker."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT DISTINCT ON (ticker)
               ticker, timestamp, composite_score, composite_score_smoothed, ema_obs_count,
               market_index, narrative_index, influencer_index, macro_index,
               market_as_of, narrative_as_of, influencer_as_of, macro_as_of
          FROM sentiment_history
         WHERE ticker = ANY($1::text[]) AND timestamp < $2
         ORDER BY ticker, timestamp DESC
        """,
        tickers, before,
    )
    return {r["ticker"]: state_from_row(dict(r)) for r in rows}


async def _done_by_tick(start: datetime, end: datetime) -> dict[datetime, set[str]]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT timestamp, ticker FROM sentiment_history "
        "WHERE replay_run = $1 AND timestamp >= $2 AND timestamp < $3",
        RUN_ID, start, end,
    )
    done: dict[datetime, set[str]] = {}
    for r in rows:
        done.setdefault(r["timestamp"], set()).add(r["ticker"])
    return done


async def replay_tick(t: datetime, tickers: list[str], sectors: dict[str, str],
                      states: dict[str, dict], skip: set[str], concurrency: int,
                      write: bool) -> dict:
    """Score every ticker as of ``t``; updates ``states`` in place."""
    stats = {"scored": 0, "failed": 0, "timed_out": 0, "skipped": len(skip)}
    sem = asyncio.Semaphore(concurrency)

    with scoring_as_of(t):
        baselines = await get_baseline_scores()

        async def _one(ticker: str) -> None:
            if ticker in skip:
                return
            async with sem:
                try:
                    state, _ = await asyncio.wait_for(
                        compute_scored_state(ticker, sectors.get(ticker),
                                             baselines.get(ticker), now=t,
                                             last_state=states.get(ticker)),
                        timeout=SCORE_TICKER_TIMEOUT_S,
                    )
                    if write:
                        await persist_replay_row(state, RUN_ID)
                    states[ticker] = state
                    stats["scored"] += 1
                except TimeoutError:
                    stats["timed_out"] += 1
                    _log.error("%s @ %s timed out — rerun to retry", ticker, t.isoformat())
                except Exception as exc:
                    stats["failed"] += 1
                    _log.warning("%s @ %s failed: %s", ticker, t.isoformat(), exc, exc_info=True)

        # Tasks copy the current context, so every ticker sees the cutoff.
        await asyncio.gather(*[_one(tk) for tk in tickers])
    return stats


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


async def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, parents=[_settings_parser()],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", type=_parse_ts, default=DEFAULT_START)
    p.add_argument("--end", type=_parse_ts, default=DEFAULT_END, help="exclusive")
    p.add_argument("--step-minutes", type=int, default=60,
                   help="tick spacing; keep it fixed across resumes (default 60)")
    p.add_argument("--concurrency", type=int, default=100)
    p.add_argument("--tickers", help="comma-separated subset (default: active universe)")
    p.add_argument("--dry-run", action="store_true", help="score the first tick only; no writes")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
    # httpx logs full request URLs at INFO — those carry the API keys.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    await init_pool(command_timeout=APP_COMMAND_TIMEOUT_S, max_size=args.concurrency + 5)
    try:
        tickers = (args.tickers.upper().split(",") if args.tickers
                   else await get_active_tickers())
        sectors = await get_ticker_sector_map()
        all_ticks = ticks(args.start, args.end, timedelta(minutes=args.step_minutes))

        done = {} if args.dry_run else await _done_by_tick(args.start, args.end)
        want = set(tickers)
        todo = [t for t in all_ticks if not want <= done.get(t, set())]
        if args.dry_run:
            todo = todo[:1]
        if not todo:
            _log.info("nothing to do — all %d ticks complete", len(all_ticks))
            return 0

        states = await _seed_states(tickers, todo[0])
        _log.info("%d/%d ticks to replay from %s (step %d min), %d tickers seeded%s",
                  len(todo), len(all_ticks), todo[0].isoformat(), args.step_minutes,
                  len(states), " [DRY RUN]" if args.dry_run else "")

        t0 = time.monotonic()
        totals = {"scored": 0, "failed": 0, "timed_out": 0}
        for i, t in enumerate(todo, 1):
            tick_t0 = time.monotonic()
            stats = await replay_tick(t, tickers, sectors, states, done.get(t, set()),
                                      args.concurrency, write=not args.dry_run)
            for k in totals:
                totals[k] += stats[k]
            el = time.monotonic() - tick_t0
            eta = (time.monotonic() - t0) / i * (len(todo) - i)
            _log.info("tick %s  %d/%d  scored=%d failed=%d timed_out=%d  %.0fs (ETA %.1fh)",
                      t.strftime("%m-%d %H:%M"), i, len(todo), stats["scored"],
                      stats["failed"], stats["timed_out"], el, eta / 3600)

        if args.dry_run:
            sample = {tk: states[tk] for tk in sorted(states)[:3] if states[tk]["timestamp"] == todo[0]}
            for tk, st in sample.items():
                _log.info("%s @ %s: score=%s raw=%s conf=%s subs=%s", tk, todo[0].isoformat(),
                          st["composite_score"], st["composite_score_raw"],
                          st["confidence"]["score"],
                          {k: (v or {}).get("value") for k, v in st["sub_indices"].items()})
        print(json.dumps({**totals, "ticks": len(todo),
                          "elapsed_s": round(time.monotonic() - t0, 1)}, indent=2))
        return 0
    finally:
        await close_pool()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

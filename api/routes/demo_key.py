"""
api/routes/demo_key.py

POST /v1/demo-key — provision (or refresh) an anonymous free-tier demo
key for the /api-access page (docs/APIACCESSPAGE.md §3.1).

Deliberately unauthenticated: this is the endpoint that hands out
credentials.  Its own protections are:

  1. Origin allowlist (SITE_ORIGINS) — browser-only by intent, but
     spoofable by non-browser clients, so it is a courtesy fence, not
     the backstop.
  2. Per-IP mint cap (Redis fixed window, DEMO_KEY_IP_CAP new mints per
     DEMO_KEY_IP_WINDOW seconds).  Validating/extending an existing key
     never consumes the cap.
  3. The hourly cleanup job pruning expired demo rows — the real bound
     on table growth.

The cap fails OPEN on a Redis outage (matching api/rate_limit.py's
degradation contract); pruning bounds the damage.
"""
from __future__ import annotations

import logging
import os
import re
import secrets

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from api.auth import _hash_token
from api.rate_limit import _INCR_WITH_TTL, _LIMITS
from api.response.schemas import DemoKeyResponse, ErrorResponse
from scripts.db.queries.api_keys import extend_demo_key, insert_demo_key
from scripts.db.redis import get_redis

_log = logging.getLogger(__name__)

router = APIRouter()

# Entries may be exact origins or wildcard patterns (`*` = one or more DNS
# labels, e.g. https://*.up.railway.app matches any Railway-hosted site).
# The list feeds BOTH the mint origin gate and main.py's CORS config, so
# they can never drift apart. An unset env var falls back to these defaults;
# if SITE_ORIGINS is set on Railway it fully replaces them.
_DEFAULT_SITE_ORIGINS = (
    "https://sentientmarkets.vercel.app,"
    "https://sentientmarkets-ai.vercel.app,"
    "https://themarketmood-ai.vercel.app,"
    "https://sentientmarkets.ai,"
    "https://www.sentientmarkets.ai,"
    # NB: *.up.railway.app is deliberately NOT a default. It is a shared
    # domain — anyone can deploy under it — so trusting it here would hand a
    # credentialed-CORS grant and demo-mint access to every Railway-hosted
    # site. If a specific Railway origin is ever genuinely needed, add it
    # explicitly via the SITE_ORIGINS env var (which fully replaces this list).
    "http://localhost:3000,"
    "http://localhost:8000"
)
SITE_ORIGINS: tuple[str, ...] = tuple(
    o.strip().rstrip("/")
    for o in os.getenv("SITE_ORIGINS", _DEFAULT_SITE_ORIGINS).split(",")
    if o.strip()
)

EXACT_ORIGINS: tuple[str, ...] = tuple(o for o in SITE_ORIGINS if "*" not in o)
_WILDCARD_ORIGINS: tuple[str, ...] = tuple(o for o in SITE_ORIGINS if "*" in o)

# `*` must only match whole DNS labels: https://*.up.railway.app matches
# https://myapp.up.railway.app but never https://evilup.railway.app or
# https://x.up.railway.app.evil.com (the regex is fully anchored).
_LABELS = r"[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*"


def _pattern_to_regex(pattern: str) -> str:
    return re.escape(pattern).replace(r"\*", _LABELS)


# Starlette's CORSMiddleware takes this as allow_origin_regex (None = no
# wildcards configured); the mint gate compiles the same expression.
CORS_ORIGIN_REGEX: str | None = (
    "^(?:" + "|".join(_pattern_to_regex(p) for p in _WILDCARD_ORIGINS) + ")$"
    if _WILDCARD_ORIGINS
    else None
)
_WILDCARD_RE = re.compile(CORS_ORIGIN_REGEX) if CORS_ORIGIN_REGEX else None


def origin_allowed(origin: str) -> bool:
    if origin in EXACT_ORIGINS:
        return True
    return bool(_WILDCARD_RE and _WILDCARD_RE.fullmatch(origin))

DEMO_KEY_IP_CAP = int(os.getenv("DEMO_KEY_IP_CAP", "5"))
DEMO_KEY_IP_WINDOW = int(os.getenv("DEMO_KEY_IP_WINDOW", "86400"))
DEMO_KEY_PREFIX = "sk-sm-free-"


class DemoKeyRequest(BaseModel):
    existing_key: str | None = None


def _client_ip(request: Request) -> str:
    """Real client IP for the per-IP mint cap.

    Railway sits in front of the app as a single trusted proxy and appends
    the true peer address as the LAST hop of X-Forwarded-For. We therefore
    take the rightmost entry, not the leftmost: the leftmost hops are
    client-supplied and trivially spoofable, which would let a caller rotate
    the mint-cap key at will. Falls back to the direct peer address."""
    xff = request.headers.get("x-forwarded-for", "")
    last = xff.split(",")[-1].strip() if xff else ""
    if last:
        return last
    return request.client.host if request.client else "unknown"


async def _check_mint_cap(ip: str) -> None:
    """Raise HTTP 429 when this IP has minted too many new keys."""
    redis_key = f"demo_mint:{ip}"
    try:
        client = get_redis()
        count = await client.eval(_INCR_WITH_TTL, 1, redis_key, DEMO_KEY_IP_WINDOW)
    except Exception as exc:
        _log.warning("demo-key mint cap check skipped (Redis unavailable): %s", exc)
        return
    if count > DEMO_KEY_IP_CAP:
        try:
            ttl = await client.ttl(redis_key)
        except Exception:
            ttl = DEMO_KEY_IP_WINDOW
        raise HTTPException(
            status_code=429,
            detail={
                "error": "demo_key_rate_limited",
                "message": (
                    "Demo key limit reached for your network. "
                    "Reuse your existing key or retry later."
                ),
                "retry_after_seconds": max(ttl, 0),
            },
        )


@router.post(
    "/demo-key",
    response_model=DemoKeyResponse,
    responses={403: {"model": ErrorResponse}, 429: {"model": ErrorResponse}},
)
async def mint_demo_key(body: DemoKeyRequest, request: Request) -> DemoKeyResponse:
    origin = (request.headers.get("origin") or "").rstrip("/")
    if not origin_allowed(origin):
        raise HTTPException(
            status_code=403,
            detail={
                "error": "origin_not_allowed",
                "message": "Requests to this endpoint must come from the site.",
            },
        )

    # Reuse path: a still-valid demo key gets its expiry refreshed and is
    # echoed back unchanged — no new row, no cap consumed.
    if body.existing_key:
        new_exp = await extend_demo_key(_hash_token(body.existing_key))
        if new_exp is not None:
            return DemoKeyResponse(
                api_key=body.existing_key,
                tier="free",
                expires_at=new_exp,
                rate_limit_per_min=_LIMITS["free"],
            )

    # Mint path.
    await _check_mint_cap(_client_ip(request))
    plaintext = DEMO_KEY_PREFIX + secrets.token_urlsafe(32)
    expires_at = await insert_demo_key(_hash_token(plaintext))
    return DemoKeyResponse(
        api_key=plaintext,
        tier="free",
        expires_at=expires_at,
        rate_limit_per_min=_LIMITS["free"],
    )

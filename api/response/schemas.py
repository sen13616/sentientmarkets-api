"""
api/response/schemas.py

Pydantic v2 response models for all API endpoints.

Models
------
    FreeTierResponse   — summary-only response (free tier)
    ProTierResponse    — full response with sub-indices, drivers, freshness
    NoDataResponse     — structured no-data / out-of-universe response
    ErrorResponse      — error body (4xx / 5xx)
    HistoryEntry       — one record in the history endpoint
    HistoryResponse    — wrapper for /history endpoint
    TickersResponse    — /v1/tickers
    StatusResponse     — /v1/status
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Market hours
# ---------------------------------------------------------------------------

class MarketHours(BaseModel):
    is_open:    bool
    next_open:  datetime
    last_close: datetime


# ---------------------------------------------------------------------------
# Sentiment — free tier
# ---------------------------------------------------------------------------

class FreeTierResponse(BaseModel):
    ticker:              str
    score:               int
    score_change_1d:     Optional[float] = None   # vs most recent tick 24-48h old; null across gaps
    score_change_1d_pct: Optional[float] = None
    label:               str
    confidence:          int
    timestamp:           datetime
    cache_age_seconds:   int
    market_hours:        MarketHours


# ---------------------------------------------------------------------------
# Sentiment — pro tier
# ---------------------------------------------------------------------------

class SubIndices(BaseModel):
    market:     Optional[float] = None
    narrative:  Optional[float] = None
    influencer: Optional[float] = None
    macro:      Optional[float] = None


class Driver(BaseModel):
    signal:       str
    description:  str
    direction:    str
    magnitude:    float
    source_layer: str


class Freshness(BaseModel):
    market_as_of:     Optional[datetime] = None
    narrative_as_of:  Optional[datetime] = None
    influencer_as_of: Optional[datetime] = None
    macro_as_of:      Optional[datetime] = None


class ProTierResponse(BaseModel):
    ticker:            str
    score:             int              # EMA-smoothed composite (with raw fallback)
    score_raw:         Optional[int]    = None   # Unsmoothed composite
    score_change_1d:     Optional[float] = None  # vs most recent tick 24-48h old; null across gaps
    score_change_1d_pct: Optional[float] = None
    universe_percentile: Optional[float] = None  # percentile within latest tick; null if not in it
    ema_obs_count:     Optional[int]    = None   # Monotonic EMA update counter
    label:             str
    confidence:        int
    sub_indices:       SubIndices
    missing_layers:    list[str]         = []
    divergence:        Optional[str]     = None
    top_drivers:       list[Driver]      = []
    explanation:       str               = ""
    freshness:         Freshness
    confidence_flags:  list[str]         = []
    timestamp:         datetime
    cache_age_seconds: int
    market_hours:      MarketHours


# ---------------------------------------------------------------------------
# No-data / out-of-universe
# ---------------------------------------------------------------------------

class NoDataResponse(BaseModel):
    ticker:  str
    status:  str    # insufficient_data | unsupported_ticker | temporarily_unavailable
    message: str


# ---------------------------------------------------------------------------
# Error
# ---------------------------------------------------------------------------

class ErrorResponse(BaseModel):
    error:   str
    message: str


# ---------------------------------------------------------------------------
# History endpoint
# ---------------------------------------------------------------------------

class HistorySubIndices(BaseModel):
    market:     Optional[float] = None
    narrative:  Optional[float] = None
    influencer: Optional[float] = None
    macro:      Optional[float] = None


class HistoryEntry(BaseModel):
    timestamp:      datetime
    score:          int                       # Smoothed (with raw fallback for pre-EMA rows)
    score_raw:      Optional[int]    = None   # Unsmoothed composite
    label:          str
    confidence:     int
    sub_indices:    HistorySubIndices
    missing_layers: list[str] = []


class HistoryResponse(BaseModel):
    ticker:  str
    history: list[HistoryEntry]


# ---------------------------------------------------------------------------
# Tickers endpoint
# ---------------------------------------------------------------------------

class TickerItem(BaseModel):
    ticker: str
    name:   Optional[str] = None
    sector: Optional[str] = None   # GICS sector (P4.1); None if not yet seeded


class TickersResponse(BaseModel):
    universe_size: int
    tickers:       list[TickerItem]


# ---------------------------------------------------------------------------
# Market overview endpoint (pro tier)
# ---------------------------------------------------------------------------

class Mover(BaseModel):
    ticker:              str
    score:               float
    score_change_1d:     Optional[float] = None
    score_change_1d_pct: Optional[float] = None


class SectorTickerRank(BaseModel):
    ticker: str
    score:  float
    rank:   int               # 1 = highest score within the sector


class SectorOverview(BaseModel):
    sector:        str
    average_score: float
    size:          int        # tickers scored in this sector this tick
    tickers:       list[SectorTickerRank]


class MarketOverviewResponse(BaseModel):
    timestamp:             datetime            # scoring tick the blob was computed at
    universe_scored:       int
    average_score:         Optional[float] = None
    breadth_above_50_pct:  Optional[float] = None
    breadth_improving_pct: Optional[float] = None   # null when no ticker has a 1d baseline
    top_movers:            list[Mover] = []
    bottom_movers:         list[Mover] = []
    sectors:               list[SectorOverview] = []


# ---------------------------------------------------------------------------
# Status endpoint
# ---------------------------------------------------------------------------

class StatusResponse(BaseModel):
    status:               str
    market_is_open:       bool
    last_market_run:      Optional[datetime] = None
    last_narrative_run:   Optional[datetime] = None
    last_influencer_run:  Optional[datetime] = None
    last_macro_run:       Optional[datetime] = None
    last_eod_run:           Optional[datetime] = None
    last_scoring_tick_run:  Optional[datetime] = None

# SESSIONB.md — API side is built & deployed: wiring guide for the website

Companion to `docs/APIACCESSPAGE.md`. Session B (this repo, `sentientmarkets-api`)
is done: commit `e04f745` on `main`, migration 013 applied to the Railway DB.
This file tells **Session A (the website)** exactly what exists, how to call it,
and how to verify the whole flow end to end.

- **API base:** `https://sentimentapi-p.up.railway.app`
- **Website env:** set `SENTIMENT_API_BASE=https://sentimentapi-p.up.railway.app`
  (reuse the existing var if the site already has one).

---

## 1. What Session B shipped

| Piece | Where | Behaviour |
|---|---|---|
| `POST /v1/demo-key` | this API, public | Mints or refreshes an anonymous free-tier demo key |
| Sliding expiry | auth path | Every authenticated request a demo key makes pushes its `expires_at` to now + 7 days |
| Cleanup job | scheduler, hourly at :50 UTC | Deletes demo keys past `expires_at`; never touches standard keys |
| CORS | API | `POST` now allowed; origins: `sentientmarkets.vercel.app`, `sentientmarkets-ai.vercel.app`, `themarketmood-ai.vercel.app`, `sentientmarkets.ai` (+`www`), **any `*.up.railway.app` site**, `localhost:3000`, `localhost:8000` |

Demo keys are `tier='free'`: **10 requests/minute**, free-tier response fields.
The plaintext looks like `sk-sm-free-<43 url-safe chars>`; only its SHA-256 is
stored server-side.

---

## 2. The contract: `POST /v1/demo-key`

**Call it from the browser, not from a Next.js server route.** The endpoint
requires the `Origin` header to be one of the allowed site origins — browsers
send it automatically on cross-origin `fetch`; server-side calls don't, and get
a 403. (This also means: no Authorization header, no cookies — nothing secret
is needed to call it.)

### Request

```
POST {SENTIMENT_API_BASE}/v1/demo-key
Content-Type: application/json

{ "existing_key": "sk-sm-free-…" }   // or null if the browser holds none
```

### 200 — success (both mint and refresh)

```json
{
  "api_key": "sk-sm-free-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "tier": "free",
  "expires_at": "2026-07-29T09:00:00Z",
  "rate_limit_per_min": 10
}
```

- If `existing_key` was still valid you get **the same key back** with a
  refreshed `expires_at` (no new key was created, no mint-cap consumed).
- If `existing_key` was null/expired/unknown, a **new** key was minted.
- Either way: **store whatever comes back.** The client never needs to
  distinguish the two cases.

### Errors — note the FastAPI `detail` wrapper

Error bodies are nested under `"detail"` (this differs from the flat shape
sketched in APIACCESSPAGE.md §3.1 — code to this real shape):

```json
// 403 — Origin header missing or not allowlisted
{ "detail": { "error": "origin_not_allowed",
              "message": "Requests to this endpoint must come from the site." } }

// 429 — this IP minted too many NEW keys (cap: 5 per 24h; refreshes don't count)
{ "detail": { "error": "demo_key_rate_limited",
              "message": "Demo key limit reached for your network. Reuse your existing key or retry later.",
              "retry_after_seconds": 79432 } }
```

On 429: show the "you've hit the demo-key limit — reuse the one you have or
try again later" state. **Do not retry in a loop**; `retry_after_seconds`
tells you when the window resets.

---

## 3. Client flow (per APIACCESSPAGE.md §5.2 — confirmed against the live API)

On `/api-access` mount:

```js
const stored = localStorage.getItem("sm_demo_key");          // may be null

const res = await fetch(`${SENTIMENT_API_BASE}/v1/demo-key`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ existing_key: stored }),
});

if (res.ok) {
  const { api_key, expires_at } = await res.json();
  localStorage.setItem("sm_demo_key", api_key);
  localStorage.setItem("sm_demo_expiry", expires_at);
  // inject api_key into the "Your free key" card AND the example curl block
} else if (res.status === 429) {
  // keep showing the stored key if one exists; render the limit message
}
```

Rules that matter:

- **Never mint just because the page loaded** — always send the stored key;
  the server decides whether to refresh or mint.
- **Regenerate button:** clear both localStorage keys, then POST with
  `existing_key: null` (subject to the 5/24h IP cap).
- A demo key that sat unused for 7+ days is gone (401 on use, then pruned).
  The mount-time POST self-heals this: the dead key falls through to a fresh
  mint. If a user gets a 401 mid-session, the fix is the same POST.

### Using the key (the example curl on the page)

```bash
curl -H "Authorization: Bearer sk-sm-free-…" \
  https://sentimentapi-p.up.railway.app/v1/sentiment/AAPL
```

Free-tier response: `ticker, score, score_raw, score_change_1d,
score_change_1d_pct, label, confidence, timestamp, cache_age_seconds,
market_hours`. Over 10 req/min → `429 {"detail": {"error":
"rate_limit_exceeded", …}}`. Every successful call slides the key's expiry
forward — active users never expire.

The contact form (§3.3) is entirely Session A's build (Resend + honeypot);
this API has no part in it.

---

## 4. End-to-end verification

### A. curl checklist (works from any terminal; `Origin` set manually)

Run in order — the whole sequence consumes **one** mint from the IP cap:

```bash
BASE=https://sentimentapi-p.up.railway.app
ORIGIN=https://sentientmarkets.vercel.app

# 1. No Origin → 403 origin_not_allowed
curl -si -X POST $BASE/v1/demo-key -H "Content-Type: application/json" \
  -d '{"existing_key": null}' | head -1          # expect HTTP 403

# 2. Mint → 200 with sk-sm-free-* key
KEY=$(curl -s -X POST $BASE/v1/demo-key -H "Content-Type: application/json" \
  -H "Origin: $ORIGIN" -d '{"existing_key": null}' | python3 -c \
  "import sys,json; print(json.load(sys.stdin)['api_key'])")
echo $KEY                                        # expect sk-sm-free-…

# 3. The key works against a real endpoint
curl -s -H "Authorization: Bearer $KEY" $BASE/v1/sentiment/AAPL | head -c 200
                                                 # expect a JSON score payload

# 4. Reuse: same key back, expires_at pushed forward, no new row
curl -s -X POST $BASE/v1/demo-key -H "Content-Type: application/json" \
  -H "Origin: $ORIGIN" -d "{\"existing_key\": \"$KEY\"}"
                                                 # expect api_key == $KEY

# 5. (Optional, burns the cap) 5 more mints with null → the last returns
#    429 demo_key_rate_limited with retry_after_seconds
```

### B. Browser checks (once the page is built)

1. Load `/api-access` → DevTools Network shows exactly **one** POST to
   `/v1/demo-key`, status 200; the key appears in the card and the curl block.
2. **Reload the page** → the POST sends the stored key and the response echoes
   it back — the displayed key must NOT change across reloads.
3. Copy-paste the example curl into a terminal → real JSON response.
4. Click Regenerate → new key appears; old one stops working within ~60s
   (auth cache TTL).
5. Hammer past the cap (6 regenerates) → the friendly 429 state renders, no
   retry loop in the Network tab.

### C. Server-side checks (owner, this repo)

- Railway logs after any hour's :50 mark:
  `demo_key_cleanup_job complete: N expired demo keys deleted` — proves the
  pruning loop is live. (Also visible as Redis key
  `pipeline:last_run:demo_key_cleanup`.)
- DB spot-check — demo rows exist, standard keys untouched:
  ```sql
  SELECT key_type, count(*), min(expires_at), max(expires_at)
    FROM api_keys GROUP BY key_type;
  ```
  Expect: 2 `standard` rows with NULL expiry (the pre-existing keys), plus
  one `demo` row per minted key, all with `expires_at` ≲ 7 days out.
- Expiry slide: note a demo key's `expires_at`, make an authenticated request
  with it, re-query — `expires_at` moved forward (granularity ~60s because of
  the auth cache).

---

## 5. Knobs (Railway env, all optional — defaults live in code)

| Var | Default | Meaning |
|---|---|---|
| `SITE_ORIGINS` | the origins in §1 | Comma-separated; feeds BOTH CORS and the mint origin gate (single-sourced). Entries may be wildcards — `*` matches one or more DNS labels, e.g. `https://*.up.railway.app` |
| `DEMO_KEY_IP_CAP` | `5` | New mints per IP per window |
| `DEMO_KEY_IP_WINDOW` | `86400` | Window seconds |
| `DEMO_KEY_TTL_DAYS` | `7` | Sliding expiry length |

Set `SITE_ORIGINS` explicitly in Railway before adding any new site origin —
one var updates CORS and the mint gate together. Abuse levers, in order:
lower `DEMO_KEY_IP_CAP`, shorten `DEMO_KEY_TTL_DAYS`, or fall back to the
shared-key design (APIACCESSPAGE.md §1a).

---

## 6. Go-live gate status (APIACCESSPAGE.md §2)

- [x] Rate limiting enforced for real (Redis Lua INCR+EXPIRE, verified in code and live)
- [x] Storage remediation Phases 0–2 done (2026-07-20)
- [x] Cleanup job deployed (verify the first `:50` log line post-deploy — §4C)

Once §4A/§4C pass, the API side is clear for Session A to flip the page live.

---

## 7. Verification record — 2026-07-22, run against production

§4A executed live after deploy of `e04f745` (migration 013 applied first):

| Check | Result |
|---|---|
| POST without Origin | ✅ 403 `origin_not_allowed` |
| POST with disallowed Origin | ✅ 403 `origin_not_allowed` |
| Mint (allowed Origin) | ✅ 200, `sk-sm-free-…`, tier `free`, 10/min, expiry +7d |
| Minted key on `/v1/sentiment/AAPL` | ✅ 200, real score payload |
| Reuse with `existing_key` | ✅ same key returned, `expires_at` slid forward |
| DB spot-check | ✅ exactly 1 `demo` row; both `standard` keys untouched (NULL expiry) |
| Cleanup job first firing | ✅ ran 2026-07-22T11:50:00Z (Redis `pipeline:last_run:demo_key_cleanup`) |

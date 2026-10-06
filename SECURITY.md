# Security policy

## Reporting a vulnerability

Please **do not open a public issue**. Report it privately via GitHub's
"Report a vulnerability" (Security → Advisories) on this repository, or contact the
maintainer directly. Include the affected endpoint or file, how to reproduce it, and the
impact. You'll get an acknowledgement within a few days.

## Scope

- The API service (`api/`): authentication, rate limiting, demo-key minting, CORS.
- The scoring pipeline and its data stores, including anything that could alter served scores.
- Secrets handling in this repository and its CI.

## Secrets

- Credentials are provided through environment variables only (`.env` locally, Railway
  variables in production). `.env` is gitignored; `.env.example` lists every variable without values.
- GitHub secret scanning and push protection are enabled; CI runs gitleaks on every push.
- API keys are stored only as SHA-256 hashes (`api_keys.key_hash`). A plaintext key is shown once,
  at creation.

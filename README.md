# Pixel Magic 2.0

Photo de-duplication pipeline (TypeScript, Temporal, Postgres). Drop photos into
`DATA_DIR/dropzone`; unique photos and copies are organised under `DATA_DIR/staging`.

Everything under `DATA_DIR` must live on one filesystem (hardlinks and renames are
relied on). Verify with `make fs-check`.

`staging/current/` is rebuilt from the database: don't keep files there. Anything saved inside
it that staging did not put there is moved to `staging/.rescued/<generation>/` when that
generation is cleaned up, never deleted.

## Development

    npm install
    npm test            # unit + workflow tests
    npm run lint && npm run format:check && npm run typecheck && npm run build

Configuration is via environment variables; see `.env.example`.

Decoding is memory-bound: `CPU_CONCURRENCY` defaults to the CPU count, capped at the
container's memory ÷ `DECODE_MEM_BUDGET_MB` (1280 MB, about one ~60 MP panorama). Docker
Desktop's VM defaults to about 8 GB, so about 6 decoders run in parallel. For more, raise the
memory under Docker Desktop → Settings → Resources, or set `CPU_CONCURRENCY` (or a smaller
`DECODE_MEM_BUDGET_MB`) in the worker's environment.

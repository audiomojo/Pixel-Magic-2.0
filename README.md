# Pixel Magic 2.0

Photo de-duplication pipeline (TypeScript, Temporal, Postgres). Drop photos into
`DATA_DIR/dropzone`; unique photos and copies are organised under `DATA_DIR/staging`.

Everything under `DATA_DIR` must live on one filesystem (hardlinks and renames are
relied on). Verify with `make fs-check`.

## Development

    npm install
    npm test            # unit + workflow tests
    npm run lint && npm run format:check && npm run typecheck && npm run build

Configuration is via environment variables; see `.env.example`.

## Photon RGB Wallet Database

This directory contains PostgreSQL schema and migration files for the Photon
multi-tenant RGB wallet backend.

Initial scope:

- wallet identity and ownership
- asset registration per wallet
- RGB invoice tracking
- RGB transfer tracking
- consignment metadata tracking
- refresh/reconciliation job tracking
- event/audit records

The first migration is:

- `migrations/001_rgb_wallets.sql`

Suggested apply flow:

```bash
psql "$PHOTON_RGB_DATABASE_URL" -f faucet/db/migrations/001_rgb_wallets.sql
```

Notes:

- This schema is designed for PostgreSQL.
- Large consignment payloads should not be stored inline in PostgreSQL by default.
- Store payload metadata, hashes, and object keys in PostgreSQL; keep large blobs
  in filesystem or object storage if needed later.

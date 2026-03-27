# PhotonBolt Regtest Faucet

This directory contains a self-contained Bitcoin regtest faucet for `faucet.photonbolt.xyz`.

## What it does

- Serves a small web UI from `faucet/public/`
- Exposes `GET /api/status` and `POST /api/claim`
- Talks to the local Bitcoin Core RPC endpoint
- Sends coins from the configured wallet
- Mines a block after each successful claim so the payment confirms immediately
- Enforces a simple cooldown per IP and per address using `faucet/data/claims.json`

## Default runtime config

- Port: `127.0.0.1:8788`
- RPC host: `127.0.0.1:18443`
- RPC wallet: `photon_dev`
- Default faucet amount: `0.5` BTC
- Allowed faucet amounts per request: `0.5`, `1`, or `2` BTC
- Cooldown: `15` minutes
- Auto-mine: `1` block

These values can be overridden with environment variables:

- `PORT`
- `BITCOIN_RPC_HOST`
- `BITCOIN_RPC_PORT`
- `BITCOIN_RPC_PROTOCOL`
- `BITCOIN_RPC_USER`
- `BITCOIN_RPC_PASSWORD`
- `BITCOIN_RPC_WALLET`
- `FAUCET_AMOUNT_BTC` (default selected amount shown in status/UI)
- `FAUCET_COOLDOWN_MINUTES`
- `FAUCET_AUTO_MINE_BLOCKS`
- `FAUCET_MINING_ADDRESS_TYPE`

You can also place backend-only secrets in `faucet/.env`. Example:

```env
BINANCE_API_KEY=
BINANCE_API_SECRET=
BINANCE_API_BASE=https://api.binance.com
```

The real `.env` file is ignored by git. Use `faucet/.env.example` as the template.

## Run manually

```bash
cd /home/waheed/PhotonBoltXYZ/faucet
node server.js
```

## API

### `GET /api/status`

Returns faucet and wallet status.

### `POST /api/claim`

Request body:

```json
{
  "address": "bcrt1...",
  "amountBtc": "0.5"
}
```

Success response includes the `txid`, amount, network, wallet, cooldown, and mined block hashes.

## Deployment files

- `faucet.service`: systemd unit for the faucet backend
- `faucet.photonbolt.xyz.conf`: Nginx vhost for the public hostname

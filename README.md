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
RGB_NODE_API_BASE=http://127.0.0.1:3001
RGB_LIGHTNING_NODE_API_BASE=http://127.0.0.1:3002
RGB_LIGHTNING_NODE_B_API_BASE=http://127.0.0.1:3003
RGB_PROXY_RPC_BASE=http://127.0.0.1:3000/json-rpc
RGB_PUBLIC_PROXY_ENDPOINT=rpcs://dev-proxy.photonbolt.xyz/json-rpc
PHOTON_ADMIN_WALLET_ADDRESS=
```

The real `.env` file is ignored by git. Use `faucet/.env.example` as the template.

### RGB endpoint configuration

Regtest and local RGB infrastructure should be configured through environment variables instead of being treated as general production defaults.

- `RGB_NODE_API_BASE`: issuer node API base
- `RGB_LIGHTNING_NODE_API_BASE`: primary user RGB Lightning node API base
- `RGB_LIGHTNING_NODE_B_API_BASE`: secondary user RGB Lightning node API base
- `RGB_PROXY_RPC_BASE`: internal JSON-RPC base used by the backend
- `RGB_PUBLIC_PROXY_ENDPOINT`: public proxy endpoint returned to clients
- `PHOTON_ADMIN_WALLET_ADDRESS`: configured admin Photon wallet address for admin-auth pages

If these values are omitted, the faucet keeps the current local regtest defaults. Set them explicitly in deployment to avoid mixing local dev assumptions with hosted environments.

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

# Quantum Sentry Trader

Rust-based HFT-style pipeline: Binance Spot/Futures → NATS → ClickHouse, plus live strategy simulation (sim-broker).

## Architecture

```
Binance (Spot + Futures WS)
        ↓
  market-ingestor   →  NATS (market.raw, market.context, market.depth.*, market.liq.*)
        ↓                        ↓
  ClickHouse  ←  market-persister   →  sim-broker (strategy + wallet)
```

| Service | Role |
|---------|------|
| **market-ingestor** | Binance WebSocket (trade, depth, mark, funding, OI, force order) → NATS. Clock sync normalizes ts_ingest to Binance time. |
| **market-persister** | NATS → ClickHouse (batch 1k/1s). Writes trade_ticks, bbo_ticks, market_context, depth_snapshots, liquidation_ticks. |
| **sim-broker** | Subscribes to NATS market.raw + market.context; runs Strategy trait (e.g. TrendFollow EMA+RSI) and Wallet simulation. |

## Requirements

- **Rust** (stable, `cargo` available)
- **Docker** (for NATS + ClickHouse)

## Quick start

**1. Infrastructure (once)**

```bash
docker-compose up -d
```

NATS: port `4222`, ClickHouse: port `8123` (HTTP).

**2. Services (in separate terminals)**

```bash
# Terminal 1
cargo run -p market-ingestor

# Terminal 2
cargo run -p market-persister

# Terminal 3 (optional)
cargo run -p sim-broker
```

Stop: `Ctrl+C` in each terminal. Tear down infrastructure: `docker-compose down`.

## Environment variables (optional)

| Variable | Default | Used by |
|----------|---------|---------|
| `NATS_URL` | `nats://127.0.0.1:4222` | ingestor, persister, sim-broker |
| `CLICKHOUSE_URL` | `http://127.0.0.1:8123` | persister |
| `CLICKHOUSE_DB` | `quantum` | persister |
| `CLICKHOUSE_USER` | `qs_user` | persister |
| `CLICKHOUSE_PASSWORD` | `qs_pass` | persister |

## ClickHouse tables

The persister creates these tables if they do not exist:

- `trade_ticks` — trades (price, qty, is_buyer_maker, ts_exchange, ts_ingest)
- `bbo_ticks` — best bid/offer
- `market_context` — mark price, funding rate, open interest
- `depth_snapshots` — L2 depth (bids/asks arrays)
- `liquidation_ticks` — force order (liquidation) events

Example check:

```bash
docker exec clickhouse-server clickhouse-client -q "SELECT count() FROM quantum.trade_ticks"
```

## Project structure

```
quantum-sentry-trader/
├── Cargo.toml          # workspace (market-ingestor, market-persister, sim-broker)
├── docker-compose.yml   # NATS + ClickHouse
├── market-ingestor/     # Binance → NATS
├── market-persister/    # NATS → ClickHouse
├── sim-broker/          # NATS → Strategy + Wallet
└── docs/                # Additional documentation
```

## Documentation

- `docs/STRATEGY_DATA_AND_ROADMAP.md` — Data set overview and strategy roadmap
- `docs/LATENCY_CHECK.md` — Depth latency queries and clock sync
- `docs/ALGORITHM_ANALYSIS.md` — Time/space complexity summary

## License

Copyright (c) 2025 Quantum Sentry Trader authors. All rights reserved.

This project is made publicly available for **non-commercial use only**. You may use, copy, and modify the source code for non-commercial purposes, provided that you **give clear attribution** to this repository and the original author(s) (e.g. link to this repo and/or author name).

**Commercial use** (including use in commercial products, services, or for profit) is **not permitted** without explicit permission from the copyright holder.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND. Full terms are in [LICENSE](LICENSE).

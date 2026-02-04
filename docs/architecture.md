# Quantum Sentinel v2 — Architecture

## Hot Path vs Cold Path

### Hot Path (RAM-only)

All trading decisions and order flow use **in-memory / stream data only**. No database access.

```
Market Gateway  →  Feature Engine  →  Executor (ONNX)  →  Orders
     (WS/REST)        (features)        (decisions)       (order commands)
```

- **Market Gateway:** Ingests market data (WebSocket/REST), publishes to NATS. No DB.
- **Feature Engine:** Consumes NATS, computes **scale-invariant** features (returns/ratios), emits `FeatureSnapshot`. No DB.
- **Executor (ONNX):** Consumes features (and optionally ticks), runs model, produces `Decision` / `OrderCommand`. No DB.
- **Orders:** Order commands sent to exchange or sim; no DB on this path.

**Rule: Executor never queries DB.** All inputs to the Executor come from the Feature Engine and live streams; ClickHouse is never read on the hot path.

**Rule: Scale-invariant features.** Use returns, ratios, and derived signals. Do **not** rely on raw price as the primary signal.

---

### Cold Path (async)

All database writes happen **asynchronously**; they do not block the hot path.

```
Async Logger  →  ClickHouse
     (NATS / internal events)
```

- **Async Logger / Persister:** Consumes NATS (or internal event bus), batches events, writes to ClickHouse. Used for: logging, backtest, replay, debugging.
- **ClickHouse:** Cold storage only. No synchronous reads or writes on the trading path.

---

## Principles (non-negotiable)

1. **Hot path never queries any database.**  
   Hot path = Market Gateway → Feature Engine → Executor (ONNX) → Orders. All decisions use RAM / stream only.

2. **ClickHouse is Cold Path only.**  
   Async Logger → ClickHouse. No DB on Gateway / Feature Engine / Executor / Orders.

3. **Executor never queries DB.**  
   Executor inputs come only from Feature Engine and live streams.

4. **Features are scale-invariant.**  
   Returns/ratios; not raw price as primary signal.

5. **Single shared contract.**  
   Event types and enums live in `rust/common`; configs in `configs/` (YAML).

---

## Repo layout (v2 foundation)

| Path | Role |
|------|------|
| **rust/common** | Shared contract: events, types, minimal YAML config loader (`config.rs`). |
| **configs/** | `features.yml`, `symbols.yml`, `risk.yml`. |
| **market-ingestor** | Market Gateway: Binance WS/REST → NATS. |
| **market-persister** | Cold Path: NATS → ClickHouse (async batch writes). |
| **sim-broker** | Executor/sim: NATS → Strategy → Wallet (no DB). |

---

## Contract layer (`rust/common`)

- **events.rs:** `TradeTick`, `BboTick`, `FeatureSnapshot`, `Decision`, `OrderCommand`.
- **types.rs:** `Action`, `OrderSide`, `OrderType`.
- **config.rs:** `SymbolsConfig`, `RiskConfig`, `FeaturesConfig`; `load_yaml`, `load_symbols`, `load_risk`, `load_features`.

---

*Sprint A — Architecture doc. Hot Path (RAM-only) vs Cold Path (async).*

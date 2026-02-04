# Quantum Sentinel Trader — Proje Bağlamı (AI / ChatGPT için A–Z Özet)

Bu belge, projenin **tam bağlamını** tek metinde toplar. ChatGPT veya başka bir asistanla çalışırken bu dosyayı vererek projeyi anlamasını sağlayabilirsiniz.

---

## 1. Proje nedir?

**Quantum Sentinel Trader (quantum-sentry-trader)** bir kripto piyasa verisi ve trading pipeline projesidir:

- **Veri kaynağı:** Binance (Spot trade/depth + Futures mark price, funding, open interest, depth, liquidation).
- **Mesajlaşma:** NATS (veri dağıtımı).
- **Kalıcı depolama:** ClickHouse (tek DB: `quantum`).
- **Dil:** Rust (tüm servisler ve kütüphaneler `rust/` altında).

**Temel kural:** Hot path (feature_engine, executor) **veritabanına bağlı değildir** — ne DB sorgusu ne de DB yazması yok. Giriş NATS’tan (process’ler arası) veya mpsc kanalından (process içi / kütüphane kullanımı) gelebilir. Veritabanına yazan bileşenler cold path’te (market-persister, isteğe logger).

**Terminoloji:** *mpsc* = process içi kanal (task/crate’ler arası). *NATS* = process’ler arası mesaj veriyolu. Asıl kısıt **“DB okuma/yazma yok”**; “sadece mpsc” değil.

---

## 2. Dizin yapısı

```
quantum-sentry-trader/
├── Cargo.toml              # Workspace; tüm rust crate'leri listelenir
├── docker-compose.yml      # NATS + ClickHouse
├── configs/                # YAML config'ler
│   ├── features.yml
│   ├── risk.yml
│   ├── logger.yml
│   ├── model.yml
│   └── symbols.yml
├── docs/                   # Dokümantasyon (STARTUP, DATA_FLOW, UNIFIED_ROLES, vb.)
├── models/                 # ONNX model meta (model_meta.json)
├── sql/                    # ClickHouse DDL referans (01–05, README)
├── scripts/                # Python (prepare_tcn_data.py vb.)
├── tests/                  # Python test (test_data_pipeline.py)
└── rust/
    ├── common/             # Paylaşılan tipler + config loader
    ├── market-ingestor/    # Binance → NATS (veri çekme)
    ├── market-persister/   # NATS → ClickHouse (ham veri yazma)
    ├── sim-broker/         # NATS dinleyen strateji + cüzdan simülasyonu
    ├── feature_engine/     # BBO → FeatureSnapshot (pencere, feature hesaplama)
    ├── executor/           # FeatureSnapshot → Decision (+ isteğe OrderCommand)
    ├── logger/             # LogEvent batch → ClickHouse (v2 tabloları)
    └── replay/             # Offline replay (ClickHouse/JSONL → pipeline → export)
```

---

## 3. Workspace crate’leri (Cargo.toml members)

- `rust/market-ingestor`
- `rust/market-persister`
- `rust/sim-broker`
- `rust/common`
- `rust/feature_engine`
- `rust/executor`
- `rust/logger`
- `rust/replay`

---

## 4. Ortak tipler (common crate)

**rust/common** tüm event ve config sözleşmesinin tek kaynağıdır.

**Olay tipleri (events.rs):**

- **TradeTick:** ts_exchange, ts_ingest, exchange_id, symbol, price, qty, is_buyer_maker
- **BboTick:** ts_exchange, ts_ingest, update_id, symbol, bid_price, bid_qty, ask_price, ask_qty
- **FeatureSnapshot:** ts_exchange, symbol, features (Vec<f32>), ready, data_latency_ms, spread_pct, feature_version
- **Decision:** ts_exchange, symbol, action (Action), confidence, reason_codes, model_version
- **OrderCommand:** client_order_id, symbol, side, order_type, qty, price, reduce_only, leverage, sl, tp
- **LogEvent:** enum — TradeTick { ts_ingest, tick }, BboTick { … }, FeatureSnapshot { … }, Decision { … }, OrderCommand { … }, ExecutionResult

**Enums (types.rs):**

- **Action:** NoTrade, Long, Short
- **OrderSide:** Buy, Sell
- **OrderType:** Market, Limit

**Config (config.rs):**

- RiskConfig (risk.yml), FeaturesConfig (features.yml), SymbolsConfig (symbols.yml)
- load_yaml, load_risk, load_features, load_symbols

Tüm zamanlar **i64 epoch millis (UTC)**.

---

## 5. Veri akışı (kim ne çeker, nereye yazar)

**Veriyi çeken tek yer:** Binance (market-ingestor).

**Dağıtan tek yer:** NATS (market-ingestor publish eder).

**Ham veri tablolarına tek yazar (single-writer):** market-persister. trade_ticks ve bbo_ticks’e **sadece** persister yazar. Logger **normal işletimde** bu raw tablolara yazmaz; sadece v2 tablolarına (feature_snapshots, decisions, order_commands) yazar. Raw tick logging (log_raw_ticks) varsa bile varsayılan kapalı olmalı veya ayrı tablolara (örn. bbo_ticks_v2_raw) yazılmalı; aksi single-writer ihlali ve çift kayıt riskidir.

**NATS subject’leri:**

- **market.raw** — Spot trade + depth (BBO) ham event’leri
- **market.context** — Futures: mark price, funding rate, open interest
- **market.depth.<symbol>** — L2 depth snapshot
- **market.liq.<symbol>** — Liquidation tick

**ClickHouse tabloları (persister yazar):**

- trade_ticks, bbo_ticks, market_context, depth_snapshots, liquidation_ticks  
**Şema kaynağı:** `rust/market-persister` içindeki `ch.rs` → `ensure_schema()`. `sql/` klasörü **referans** olup ensure_schema ile aynı tutulmalıdır; şema değişikliği yaparken hem ensure_schema hem ilgili sql/ dosyaları aynı PR’da güncellenir. trade_ticks / bbo_ticks: PARTITION BY toDate(ts_exchange). depth_snapshots / liquidation_ticks: PARTITION BY toYYYYMMDD(ts_exchange).

**v2 logger (opsiyonel)** ayrı tablolara yazar: feature_snapshots, decisions, order_commands (sql/03–05 referans).

---

## 6. Crate bazında özet

### market-ingestor

- **Görev:** Binance’ten veri çekip NATS’a publish etmek. Hiçbir DB’ye yazmaz.
- **Kaynaklar:** Spot WebSocket (trade, depth), Futures WebSocket (mark, funding, OI, depth, liq), REST (ticker/24hr → top 50 USDT sembol listesi), clock sync.
- **Çıktı:** market.raw, market.context, market.depth.<s>, market.liq.<s>.
- **Ana dosyalar:** main.rs, stream.rs, messaging.rs, model.rs, universe.rs.

### market-persister

- **Görev:** NATS’ı dinleyip parse edip ClickHouse’a yazmak. trade_ticks / bbo_ticks vb. tabloların tek yazarı.
- **Dinlediği:** market.raw, market.context, market.depth.*, market.liq.*.
- **ClickHouse:** ensure_schema() ile tabloları oluşturur; insert_*_batch ile yazar. DB: quantum (ortam değişkeni: **CLICKHOUSE_DB**; eski CLICKHOUSE_DATABASE desteklenir, deprecation uyarısı verilir).
- **Ana dosyalar:** main.rs, ch.rs, model.rs.

### sim-broker

- **Görev:** NATS market.raw + market.context dinleyerek strateji ve cüzdan simülasyonu. DB’ye yazmaz.
- **Ana dosyalar:** main.rs, listener.rs, strategy.rs, wallet.rs, model.rs.

### common

- **Görev:** Tüm crate’lerin kullandığı event tipleri (BboTick, TradeTick, FeatureSnapshot, Decision, OrderCommand, LogEvent), enums (Action, OrderSide, OrderType), config tipleri ve YAML loader.
- **Ana dosyalar:** lib.rs, events.rs, types.rs, config.rs.

### feature_engine

- **Görev:** BBO akışı → pencere bazlı feature hesaplama → FeatureSnapshot. Hot path; DB yok.
- **Giriş:** BboTick — process içi kullanımda mpsc kanalı; standalone binary’de NATS market.raw subscribe + parse.
- **Çıkış:** FeatureSnapshot (kanal; try_send, doluysa drop).
- **Kavramlar:** Ring buffer, spread_pct, log return, imbalance; window (config’ten veya env).
- **Ana dosyalar:** main.rs (NATS subscribe), runner.rs (channel_pair, run), state.rs (FeatureEngine), features.rs, ring_buffer.rs.

### executor

- **Görev:** FeatureSnapshot → gate’ler (latency, spread, confidence, cooldown, hysteresis) → inference (Mock veya ONNX) → Decision; execution_enabled ise OrderCommand. Hot path; DB yok.
- **Config:** ExecutorConfig (risk.yml: execution_enabled, latency_max_ms, spread_max_pct, confidence_min, cooldown_ms, hysteresis_enter/exit).
- **Inference:** InferenceEngine trait; MockInferenceEngine (test/varsayılan); opsiyonel ONNX (onnx feature).
- **Ana dosyalar:** main.rs, runner.rs, executor.rs, config.rs, inference.rs, model_config.rs, onnx_engine.rs.

### logger

- **Görev:** LogEvent’leri kanaldan alıp batch’leyip ClickHouse’a yazmak (cold path). feature_engine/executor çalıştırıyorsanız opsiyonel.
- **Config:** LoggerConfig (logger.yml veya env): enabled, flush_interval_ms, max_batch_size, drop_on_full, log_raw_ticks, clickhouse (url, database, …).
- **Sink:** MockSink (test); ClickHouseSink (clickhouse feature). Tablolar: feature_snapshots, decisions, order_commands. **Normal işletimde** trade_ticks/bbo_ticks’e yazmaz; log_raw_ticks açıksa mevcut kod raw tablolara yazabilir — production’da kapalı tutulmalı veya ayrı tablolara yönlendirilmelidir.
- **Ana dosyalar:** lib.rs, runtime.rs (run_logger, try_send_log), sink.rs, config.rs.

### replay

- **Görev:** Offline deterministik replay: ClickHouse veya JSONL’den tick oku → feature_engine → executor → çıktıları export et. Canlı borsa/NATS gerekmez.
- **Kaynak:** ReplaySource trait; ClickHouseReplaySource (bbo_ticks, isteğe trade_ticks), JsonlReplaySource (test için).
- **Çıktı:** export_dir’e features.*, decisions.*, summary.json; isteğe golden/<name>/raw_bbo.jsonl, expected_features.jsonl, expected_decisions.jsonl.
- **CLI (clap):** --symbol, --from, --to, --source clickhouse|jsonl, --jsonl_path, --export_dir, --export_format csv|jsonl|parquet, --replay_speed, --limit, --log_raw_ticks, --write_decisions, --golden_name, --execution_enabled, --features_config, --risk_config, --model_config, --model_meta.
- **Config:** configs/features.yml (window), configs/risk.yml (ExecutorConfig).
- **Ana dosyalar:** main.rs, engine.rs (run_replay), export.rs, source.rs, source_clickhouse.rs, source_jsonl.rs, tests/replay_tests.rs.

---

## 7. Config dosyaları

- **configs/features.yml:** version, features (name, dtype, window, missing_policy). feature_engine window buradan veya env.
- **configs/risk.yml:** execution_enabled, latency_max_ms, spread_max_pct, confidence_min, cooldown_ms, hysteresis_enter/exit, max_daily_loss_pct, max_risk_per_trade_pct, max_leverage. executor ve replay kullanır.
- **configs/logger.yml:** enabled, flush_interval_ms, max_batch_size, drop_on_full, log_raw_ticks, clickhouse (url, database, credentials, send_retries, retry_backoff_ms).
- **configs/model.yml:** ONNX model path, meta path (executor onnx feature).
- **configs/symbols.yml:** symbols listesi (isteğe).

---

## 8. Ortam değişkenleri (özet)

- **NATS_URL** (varsayılan: nats://127.0.0.1:4222): ingestor, persister, sim-broker, feature_engine.
- **NATS_SUBJECT** (market.raw), **NATS_SUBJECT_CONTEXT** (market.context): persister, feature_engine.
- **CLICKHOUSE_URL**, **CLICKHOUSE_DB** (canonical; CLICKHOUSE_DATABASE eski adı desteklenir, deprecation uyarısı), **CLICKHOUSE_USER**, **CLICKHOUSE_PASSWORD**: persister, logger, replay (clickhouse source). Varsayılanlar docker-compose içinde.
- **FEATURE_WINDOW**, **EXECUTION_ENABLED**, **LOGGER_CONFIG**, **EXECUTOR_CONFIG**, **LOGGER_ENABLED**, vb.: ilgili bileşenlerde dokümantasyonda geçer.

---

## 9. Çalıştırma modları (Operating Modes)

**Mode A — Legacy / Base (önerilen varsayılan):**  
market-ingestor → NATS → market-persister → ClickHouse (raw tablolar). İsteğe sim-broker. feature_engine / executor / logger **kapalı**.

**Mode B — v2 Hot Path + Cold Path logging (persister ile birlikte):**  
market-ingestor → NATS. feature_engine NATS market.raw’a subscribe (veya process içi mpsc). executor snapshot tüketir. logger **sadece v2 tablolarına** yazar (feature_snapshots, decisions, order_commands). market-persister aynı anda çalışabilir; **log_raw_ticks kapalı** olmalı (single-writer: trade_ticks/bbo_ticks’e sadece persister yazar).

**Mode C — v2-only, persister yok (ileri / varsayılan değil):**  
Sadece istemeyle: logger’ın raw tick yazması gerekiyorsa, **ayrı tablolara** (örn. bbo_ticks_v2_raw, trade_ticks_v2_raw) yazılmalı; aynı trade_ticks/bbo_ticks tablolarına yazmak single-writer ihlalidir. Önerilmez; gerekçen varsa kullan.

**Kural:** trade_ticks ve bbo_ticks **single-writer**: sadece market-persister. Logger normal işletimde bu raw tablolara yazmaz.

---

## 10. Çalıştırma sırası

1. **Altyapı:** `docker-compose up -d` (NATS + ClickHouse).
2. **Canlı veri + simülasyon (Mode A):**  
   `cargo run -p market-ingestor`  
   `cargo run -p market-persister`  
   `cargo run -p sim-broker`  
   (Üçü aynı NATS’a bağlanır; persister ClickHouse’a yazar.)
3. **v2 (Mode B):** feature_engine (NATS market.raw gerekir), executor, isteğe logger (ClickHouse quantum + v2 tabloları; log_raw_ticks=false).
4. **Offline replay:** `cargo run -p replay -- --source clickhouse --symbol BTCUSDT --from "..." --to "..." --export_dir exports/run_001` veya `--source jsonl --jsonl_path <path>` (ClickHouse/NATS gerekmez).

---

## 11. Testler

- **replay:** JsonlReplaySource ile sıralama, deterministik karar, exporter, golden (ClickHouse gerekmez).
- **logger:** flush by size/timer, drop_on_full, reason_codes (tokio test).
- **executor:** gate’ler, shadow/execution, cooldown, hysteresis, meta_mismatch, inference_error.
- **feature_engine:** state, ring_buffer, features birim testleri.
- **sim-broker:** wallet (fee, slippage, insufficient funds).
- **market-ingestor:** build_url, universe filtering.

Tüm testler **canlı NATS/ClickHouse gerektirmez**; replay testleri JSONL kullanır.

Komut: proje kökünde `cargo test` veya `cargo test -p <crate>`.

---

## 12. Önemli kısıtlar ve kurallar

- **Hot path DB yok:** feature_engine ve executor **veritabanı sorgusu veya yazması yapmaz**. Giriş NATS’tan (inter-process) veya mpsc’den (in-process) gelebilir; kısıt “DB kullanımı yok”tur, “sadece mpsc” değil.
- **Single-writer raw tablolar:** trade_ticks ve bbo_ticks’e **sadece** market-persister yazar. Logger **normal işletimde** bu tablolara yazmaz; sadece v2 tablolarına (feature_snapshots, decisions, order_commands) yazar. Raw tick logging açıksa çift kayıt ve single-writer ihlali riski vardır; production’da kapalı veya ayrı tablolara yönlendirilmelidir.
- **Şema kaynağı:** ClickHouse şema gerçeği = **market-persister ch.rs ensure_schema()**. sql/ referans olup buna uymalıdır; şema değişikliğinde hem ensure_schema hem ilgili sql/ dosyaları aynı PR’da güncellenir.
- **Tek DB:** Tüm ClickHouse verisi `quantum` DB’sinde. Ortam değişkeni: **CLICKHOUSE_DB** (CLICKHOUSE_DATABASE eski adı desteklenir, deprecation uyarısı).
- **Replay deterministik:** Aynı girdi → aynı feature ve decision; duvar saati sadece replay_speed > 0 için gecikme için kullanılır.
- **Rust crate’leri:** Hepsi `rust/` altında; workspace root’taki Cargo.toml’da members olarak listelenir.
- **Gecikme:** Hedef/aspirasyon ölçülerek doğrulanmalıdır. Process içi mpsc en hızlı; NATS ek gecikme ekler (kabul edilebilir ama aynı değil).

---

## 13. Ölçek-bağımsız feature’lar (Price Scale Invariance)

Model ve feature’lar **ham fiyatı birincil sinyal olarak kullanmaz**; log getiri, oran ve spread kullanılır. Böylece 1000$ ve 0.00035$ coin’ler karşılaştırılabilir.

- **Kullanılan:** log return (1-tick, pencere), spread_pct, imbalance.
- **Feature sırası (Rust ile Python eğitimi aynı olmalı):**  
  `["log_return_1", "log_return_window", "spread_pct", "imbalance"]`  
  (feature_engine state.rs içinde bu sıra: lr1, lr_window, last_spread_pct, last_imbalance.)
- **feature_version** ve sıra, canlı inference ile eğitim pipeline’ında **aynı** tutulmalı; golden testler (replay golden pack) parity’yi zorunlu kılar.

---

## 14. Dokümantasyon (docs/)

- **STARTUP.md:** Sistemi ayağa kaldırma (docker, 3 servis, env).
- **DATA_FLOW_AND_RUN.md:** Veri nereden geliyor, nereye yazılıyor, hangi senaryoda ne çalıştırılır.
- **UNIFIED_ROLES.md:** Kim ne yapar, tek sorumluluk, çakışan yazar yok.
- **sql/README.md:** SQL dosyalarının amacı (persister DDL vs. manuel referans).
- **PROJECT_CONTEXT_FOR_AI.md:** Bu dosya (A–Z proje özeti).

Bu belge ile bir AI asistanı projenin yapısını, veri akışını, crate sorumluluklarını ve çalıştırma/test bağlamını tek metinden çıkarabilir.

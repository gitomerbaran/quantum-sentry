# Veri nereden geliyor, nereye yazılıyor, neyi çalıştırıyoruz?

Bu belge: **hangi veriyi nasıl çektiğimiz**, **nereye kaydettiğimiz** ve **hangi sırayla ne çalıştıracağınız** özeti.

---

## 1. Veri kaynağı: Binance

Tüm piyasa verisi **Binance**’ten geliyor:

| Kaynak | Ne çekiyoruz | Nasıl |
|--------|----------------|------|
| **Binance Spot** | Trade + depth (BBO) stream’leri | WebSocket (combined stream): `btcusdt@trade`, `btcusdt@depth@100ms` vb. |
| **Binance REST** | İlk sembol listesi (top 50 USDT hacim) | `GET /api/v3/ticker/24hr` → quote volume’e göre sıralayıp sembolleri seçiyoruz |
| **Binance Futures** | Mark price, funding rate, open interest, L2 depth, liquidation | WebSocket + REST (OI cache için) |

Yani: **Spot** trade/BBO + **Futures** context/depth/liquidation. Hepsi **market-ingestor** tek serviste toplanıyor.

---

## 2. İlk durak: NATS (mesajlaşma)

**market-ingestor** veriyi **NATS**’a yayımlıyor. Hiçbir veritabanına yazmıyor.

| NATS subject | İçerik | Kim publish ediyor |
|--------------|--------|---------------------|
| **market.raw** | Ham Binance event’leri (trade + depthUpdate tek akışta) | market-ingestor (Spot stream) |
| **market.context** | Futures: mark price, funding rate, open interest (normalize JSON) | market-ingestor (Futures context stream) |
| **market.depth.\<symbol\>** | L2 depth snapshot (Futures, per symbol) | market-ingestor |
| **market.liq.\<symbol\>** | Liquidation tick (Futures, per symbol) | market-ingestor |

Özet: **Veriyi çeken tek yer Binance; dağıtan tek yer NATS (ingestor publish ediyor).**

---

## 3. İkinci durak: ClickHouse (kalıcı depolama)

**market-persister** NATS’ı dinliyor, parse edip **ClickHouse**’a yazıyor. Bu tablolara yazan **sadece persister**.

| ClickHouse tablosu | İçerik | NATS kaynağı |
|--------------------|--------|---------------|
| **trade_ticks** | Gerçekleşen işlemler (ts_exchange, ts_ingest, trade_id, symbol, price, quantity, is_buyer_maker) | market.raw (trade event’leri) |
| **bbo_ticks** | Best bid/offer (ts_exchange, ts_ingest, update_id, symbol, bid/ask price & qty) | market.raw (depthUpdate → BBO) |
| **market_context** | Futures: ts, symbol, mark_price, funding_rate, open_interest, next_funding_time | market.context |
| **depth_snapshots** | L2 depth (bids/asks array’leri) | market.depth.* |
| **liquidation_ticks** | Zorla kapatma event’leri | market.liq.* |

- **Veritabanı adı:** `quantum` (docker-compose ile gelir; ortam değişkeni **CLICKHOUSE_DB**; eski CLICKHOUSE_DATABASE desteklenir, deprecation uyarısı).
- **Şema kaynağı:** Persister’daki `ch.rs` → `ensure_schema()`. sql/ referans olup buna uymalıdır; şema değişikliğinde hem ensure_schema hem sql/ aynı PR’da güncellenir. trade_ticks/bbo_ticks: PARTITION BY toDate(ts_exchange); depth_snapshots/liquidation_ticks: PARTITION BY toYYYYMMDD(ts_exchange).

---

## 4. Kim neyi dinliyor / neye ihtiyaç duyuyor?

| Bileşen | Dinlediği / kullandığı | Nereye yazar (varsa) |
|---------|------------------------|----------------------|
| **market-ingestor** | Binance (WS + REST) | Hiçbir yere; sadece NATS’a publish |
| **market-persister** | NATS: market.raw, market.context, market.depth.*, market.liq.* | ClickHouse: trade_ticks, bbo_ticks, market_context, depth_snapshots, liquidation_ticks |
| **sim-broker** | NATS: market.raw, market.context | Hiçbir DB’ye yazmaz (sadece strateji + cüzdan simülasyonu) |
| **feature_engine** | NATS: market.raw (BBO parse eder) | Hiçbir yere (snapshot’ları sadece kanalda; isteğe logger) |
| **executor** | feature_engine’den gelen FeatureSnapshot (kanal) | Hiçbir yere (Decision/OrderCommand; isteğe logger) |
| **logger** | Kullanılmaz doğrudan; feature_engine/executor LogEvent kanala atar | ClickHouse (quantum): sadece feature_snapshots, decisions, order_commands. trade_ticks/bbo_ticks’e normal işletimde yazmaz (single-writer = persister). Raw tick logging production’da kapalı veya ayrı tablolara. |
| **replay** | ClickHouse’tan veya JSONL dosyadan (offline) | Sadece disk: export_dir (features, decisions, summary, golden) |

Özet:
- **Canlı veri** = NATS (ingestor doldurur, persister + sim-broker + isteğe feature_engine dinler).
- **Kalıcı ham veri** = ClickHouse quantum (persister yazar; replay okur).
- **v2 pipeline çıktıları** = logger ile ClickHouse’a (ayrı tablolar) veya replay ile diske.

---

## 5. Çalıştırma modları (Operating Modes)

- **Mode A — Legacy/Base:** ingestor → NATS → persister → ClickHouse (raw tablolar); isteğe sim-broker. feature_engine/executor/logger kapalı.
- **Mode B — v2 + persister:** Aynı 3 servis + feature_engine, executor, isteğe logger. Logger **sadece v2 tablolarına** yazar; **log_raw_ticks kapalı** (trade_ticks/bbo_ticks’e tek yazar persister).
- **Mode C — v2-only (ileri):** Persister yok; logger raw tick yazacaksa **ayrı tablolara** (örn. bbo_ticks_v2_raw) yazılmalı. Önerilmez.

**Kural:** trade_ticks ve bbo_ticks single-writer: sadece market-persister. Logger bu raw tablolara normal işletimde yazmaz.

---

## 6. Ne çalıştırıyorsun? (Sıra ve bağımlılıklar)

### Altyapı (bir kez)

```powershell
docker-compose up -d
```

- **NATS** → 4222 (client), 8222 (monitoring)
- **ClickHouse** → 8123 (HTTP), 9000 (native), DB: `quantum`

Tabloları sen oluşturmuyorsun; **market-persister** ilk açılışta `ensure_schema()` ile oluşturur.

---

### Senaryo A: Sadece canlı veri + simülasyon (3 servis)

Sıra önemli değil ama hepsi aynı NATS’a bağlanır; persister ClickHouse’a yazar.

1. **market-ingestor**  
   Binance’ten çeker, NATS’a publish eder (market.raw, market.context, market.depth.*, market.liq.*).

2. **market-persister**  
   NATS’ı dinler, parse eder, ClickHouse’a yazar (trade_ticks, bbo_ticks, market_context, depth_snapshots, liquidation_ticks).

3. **sim-broker**  
   market.raw + market.context dinler; strateji + cüzdan simülasyonu (DB yok).

Bunlar çalışınca: **Veri Binance → NATS → ClickHouse’ta**; sim-broker canlı strateji çalıştırır.

---

### Senaryo B: v2 pipeline (feature_engine + executor)

feature_engine ve executor **NATS’tan ham veri çekmez**; feature_engine NATS market.raw’dan BBO alır, executor feature_engine çıktısını alır.

- **feature_engine** çalıştırırsan: NATS market.raw’a subscribe olur, BBO parse eder, FeatureSnapshot üretir (kanala). İstersen bu snapshot’ları **logger** ile ClickHouse’a yazdırırsın.
- **executor** çalıştırırsan: Genelde feature_engine’in çıktısına bağlı (kanal veya aynı process’te). Snapshot → Decision (+ isteğe OrderCommand); bunları da **logger** ile ClickHouse’a yazabilirsin.

Yani v2 için:
- **NATS** (market.raw) + **market-ingestor** gerekir ki feature_engine BBO görsün.
- **ClickHouse** zorunlu değil feature_engine/executor için; ama logger kullanırsan **quantum** DB ve ilgili tablolar (feature_snapshots, decisions, order_commands) gerekir.

---

### Senaryo C: Offline replay (ClickHouse’ta birikmiş veriyle)

- **replay** çalıştırırsan: **ClickHouse**’tan (trade_ticks / bbo_ticks) veya **JSONL** dosyadan okur. NATS ve Binance gerekmez.
- Gerekli olan: **ClickHouse’ta** ilgili symbol ve zaman aralığı için **bbo_ticks** (ve isteğe trade_ticks) dolu olsun; veya JSONL ile test.

Örnek:

```powershell
cargo run -p replay -- --source clickhouse --symbol BTCUSDT --from "2026-01-01T00:00:00Z" --to "2026-01-01T01:00:00Z" --export_dir exports/run_001
```

Veya JSONL ile (ClickHouse yok):

```powershell
cargo run -p replay -- --source jsonl --jsonl_path path/to/events.jsonl --export_dir exports/run_001
```

---

## 7. Ortam değişkenleri (özet)

| Değişken | Varsayılan | Kullanan |
|----------|------------|----------|
| NATS_URL | nats://127.0.0.1:4222 | ingestor, persister, sim-broker, feature_engine |
| NATS_SUBJECT | market.raw | persister, feature_engine |
| NATS_SUBJECT_CONTEXT | market.context | persister |
| CLICKHOUSE_URL | docker-compose / ortam | persister, logger, replay (clickhouse source) |
| CLICKHOUSE_DB | docker-compose / ortam | persister, logger, replay (canonical; CLICKHOUSE_DATABASE eski adı desteklenir, deprecation uyarısı) |

Config dosyaları: `configs/` (features.yml, risk.yml, logger.yml, model.yml, symbols.yml). Detay için `docs/STARTUP.md`.

---

## 8. Kısa cevap: “Neleri çalıştıracağız?”

- **Sadece veri biriktirip simülasyon:**  
  `docker-compose up -d` → `market-ingestor` → `market-persister` → `sim-broker`.

- **Üzerine v2 (feature + karar):**  
  Aynı 3’ü çalıştır; ek olarak `feature_engine` (NATS market.raw gerekir). İstersen `executor` + `logger` (ClickHouse quantum + v2 tabloları gerekir).

- **Sadece offline analiz / dataset:**  
  ClickHouse’ta veri varsa `replay` (clickhouse source); yoksa JSONL ile `replay` (ClickHouse ve NATS gerekmez).

Veriyi **çeken** tek yer **Binance**, **dağıtan** tek yer **NATS**, **ClickHouse’a ham veriyi yazan** tek yer **market-persister**; diğer her şey bu akışa bağlı veya opsiyonel.

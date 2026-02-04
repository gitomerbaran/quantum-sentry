# Tek mimari: Kim ne yapıyor, aynı iş iki yerde yok

Bu belge, çalışan 3 script ile v2 bileşenlerinin rollerini netleştirir. **Aynı işi yapan iki farklı şey yok**; her veri akışı tek sorumluda.

---

## Çalıştırdığınız 3 script (veri + simülasyon)

| Script | Görevi | Yazdığı yer |
|--------|--------|-------------|
| **market-ingestor** | Binance WS → NATS (market.raw, market.context, market.depth.*, market.liq.*) | Hiçbir DB’ye yazmaz; sadece NATS’a publish eder. |
| **market-persister** | NATS’ı dinler → trade/BBO/context/depth/liq’i parse eder → **ClickHouse’a yazar** | **trade_ticks, bbo_ticks, market_context, depth_snapshots, liquidation_ticks** (tek yazar bu tablolara). |
| **sim-broker** | NATS market.raw + market.context’i dinler → strateji + cüzdan simülasyonu | Hiçbir DB’ye yazmaz. |

- **Veriyi çeken / DB’ye yazan tek servis:** market-persister.  
- trade_ticks ve bbo_ticks’e **sadece persister** yazar (NATS’tan gelen ham akıştan).  
- ClickHouse DB: **quantum** (docker-compose ve persister aynı DB’yi kullanır).

---

## v2 bileşenleri (ekstra; 3 script’e alternatif değil)

| Bileşen | Ne yapar | Ne zaman kullanılır |
|---------|----------|---------------------|
| **feature_engine** | NATS market.raw’dan BBO parse eder → feature snapshot üretir | İleride model/executor pipeline’ı kurulduğunda; **şu an çalışan 3 script’in yerine geçmez**. |
| **executor** | Feature snapshot (veya mock) → karar/ONNX | Ayrı bir process; persister/sim-broker’ı değiştirmez. |
| **logger** (rust/logger) | Feature_engine veya executor’dan gelen **LogEvent**’leri batch’leyip ClickHouse’a yazar | Sadece feature_engine veya executor çalıştırıyorsanız, **opsiyonel**. Yazar: **sadece** feature_snapshots, decisions, order_commands. trade_ticks/bbo_ticks’e normal işletimde yazmaz; log_raw_ticks production’da kapalı veya ayrı tablolara. |

- **trade_ticks ve bbo_ticks single-writer: sadece market-persister.** Logger **normal işletimde** bu raw tablolara yazmaz; sadece v2 tablolarına (feature_snapshots, decisions, order_commands) yazar.  
- log_raw_ticks açıksa mevcut kod raw tablolara yazabilir: **production’da kapalı** önerilir (çift kayıt + single-writer ihlali). Açacaksanız raw tick’leri **ayrı tablolara** (örn. bbo_ticks_v2_raw) yönlendirin.

---

## Şema kaynağı (source of truth)

- **ClickHouse şema gerçeği = market-persister ch.rs ensure_schema().**  
- sql/ **referans** olup ensure_schema ile **aynı** tutulmalıdır. Şema değişikliği yaparken **hem ensure_schema hem ilgili sql/ dosyalarını aynı PR’da güncelleyin.**  
- trade_ticks / bbo_ticks: PARTITION BY toDate(ts_exchange). depth_snapshots / liquidation_ticks: PARTITION BY toYYYYMMDD(ts_exchange).

---

## Özet

- **Veri çekme + ClickHouse’a yazma (trade_ticks, bbo_ticks, …):** Sadece **market-persister**.  
- **Strateji simülasyonu (NATS’tan canlı):** Sadece **sim-broker**.  
- **v2 (feature_engine, executor, logger):** Ek pipeline; 3 script’i değiştirmez, aynı işi ikinci kez yapan servis yok. Logger aynı DB’yi (quantum) kullanır ama farklı tablolara (ve isteğe bağlı raw tick) yazar.

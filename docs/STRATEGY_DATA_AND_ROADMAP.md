# Strateji: Veri Seti ve Yol Haritası

Kusursuz veri seti hazır; strateji katmanı bu veriyi nasıl kullanacak, ne üretecek — özet ve yol haritası.

---

## 1) Hazır veri seti (pipeline çıktısı)

### ClickHouse tabloları (kalıcı, backtest / analiz)

| Tablo | İçerik | Strateji için |
|-------|--------|----------------|
| `trade_ticks` | İşlem fiyatı, miktar, is_buyer_maker, ts_exchange, ts_ingest | Fiyat serisi, agresör yönü (pump/dump), latency |
| `bbo_ticks` | Best bid/ask, update_id, ts_exchange, ts_ingest | Spread, order book girişi |
| `market_context` | mark_price, funding_rate, open_interest, next_funding_time | Mark fiyat, funding carry, OI |
| `depth_snapshots` | bids/asks price+qty array’leri, ts_exchange, ts_ingest | L2 derinlik, spread, imbalance |
| `liquidation_ticks` | side, price, orig_qty, last_filled_qty | Likidasyon dalgaları, piyasa stresi |

### NATS (canlı, sim-broker)

| Subject | İçerik | Şu an kullanım |
|---------|--------|-----------------|
| `market.raw` | Trade + BBO (CombinedStream) | ✅ Strateji fiyat + BBO alıyor |
| `market.context` | mark_price, funding, OI | ✅ Wallet/context güncelleniyor |
| `market.depth.*` | DepthSnapshot (L2) | ❌ Henüz stratejiye bağlı değil |
| `market.liq.*` | LiquidationTick | ❌ Henüz stratejiye bağlı değil |

---

## 2) Mevcut strateji (sim-broker)

- **Girdi:** `market.raw` (trade/BBO) + `market.context` (mark, funding, OI).
- **Mantık:** `TrendFollowStrategy` — EMA(50) + RSI(14); trend yönünde pullback’te long, aşırı alımda kapat.
- **Çıktı:** `TradeAction` (OpenLong, OpenShort, ClosePosition, Hold) → wallet’ta pozisyon aç/kapa.

Eksik kullanılan veri: depth (L2), liquidations. Bunlar veri setinde var, strateji tarafında henüz kullanılmıyor.

---

## 3) Strateji yol haritası (veri → sinyal)

Veri setini “kusursuz” kabul edip strateji tarafını netleştirmek için:

1. **Backtest altyapısı**
   - ClickHouse’tan zaman sıralı okuma: `trade_ticks`, `bbo_ticks`, `market_context`, isteğe `depth_snapshots`, `liquidation_ticks`.
   - Aynı `Strategy` trait’ini backtest motorunda çalıştır (event-by-event veya bar’a indirgeyerek).
   - Metrik: sharpe, drawdown, win rate, latency dağılımı.

2. **Depth kullanımı**
   - `market.depth.*` veya ClickHouse `depth_snapshots`: spread, bid/ask imbalance (imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty)).
   - Strateji: spread dar + bid imbalance → long bias; ask imbalance → short bias; veya sadece filtre (aşırı spread’te işlem yapma).

3. **Liquidations**
   - `market.liq.*` veya `liquidation_ticks`: ani likidasyon artışı = piyasa stresi, tersine dönüş fırsatı veya momentum.
   - Strateji: likidasyon spike’ı sonrası mean reversion veya momentum devamı (test edilecek).

4. **Funding / OI**
   - Zaten `market.context` ile geliyor; mark_price, funding_rate, open_interest.
   - Strateji: yüksek funding’de ters pozisyon (funding carry), OI düşüşü = long’lar kapanıyor (bearish) gibi okumalar.

5. **Latency bilinci**
   - `ts_ingest`, `ts_exchange` ile gerçek gecikme ölçülüyor; strateji kararı “geç” kalıyorsa sinyal kalitesi düşer. Backtest’te gecikme simülasyonu (örn. N ms gecikmeli execution) eklenebilir.

---

## 4) Kısa özet

- **Veri seti:** Trade, BBO, context, depth, liquidations — hepsi tek pipeline’da, clock sync ile anlamlı latency.
- **Strateji:** Şu an trade + context kullanılıyor; depth ve liq eklenebilir; backtest ClickHouse üzerinden yapılabilir.
- **Sonraki adım:** Backtest motoru (ClickHouse → event stream → Strategy) veya canlıda `market.depth.*` / `market.liq.*` aboneliği ile strateji girdisini genişletmek — hangisini önce almak istediğine göre ilerlenebilir.

# Quantum-Sentry-Trader — Algoritma Analizi

Kod değişikliği yapılmadan, zaman/uzay karmaşıklığı ve maliyet özeti.

---

## 1. market-ingestor

### 1.1 Universe (fetch_top_50_coins)

| İşlem | Zaman | Uzay | Açıklama |
|-------|--------|------|----------|
| HTTP GET + JSON | O(1) ağ | O(R) | R = ticker sayısı (~binler) |
| USDT filtre + parse | O(R) | O(R) | ranked Vec |
| sort_by volume | O(R log R) | O(1) ek | Karşılaştırma O(1) |
| take(50) + sort + dedup | O(50 log 50) | O(50) | S = 50 sembol |

**Toplam:** O(R log R) per 15 dakika. Günlük maliyet ihmal edilebilir.

### 1.2 Stream (build_url)

| İşlem | Zaman | Uzay |
|-------|--------|------|
| S sembol → 2S stream | O(S) | O(S) |
| join("/") | O(S) | O(|URL|) |

S = 50 ⇒ O(50), sabit.

### 1.3 Hot path (handle_text_message — mesaj başına)

| İşlem | Zaman | Uzay |
|-------|--------|------|
| from_str CombinedStream\<Value> | O(L) | O(L) | L = mesaj uzunluğu (byte) |
| data.get("e") | O(1) | O(1) |
| from_str typed (trade/depth) | O(L) | O(L) | İkinci parse |
| json! + to_vec | O(L) | O(L) |
| nats.publish | O(L) ağ | O(1) |

**Mesaj başına:** O(L) zaman, O(L) geçici bellek (parse + serialize).  
L tipik 200–2000 byte (trade ~300, depth ~500–2k).

---

## 2. sim-broker

### 2.1 Listener (mesaj başına)

| İşlem | Zaman | Uzay |
|-------|--------|------|
| serde_json::from_slice | O(L) | O(L) |
| evt.symbol() / price_f64() | O(1) | O(1) | Value get + parse |
| wallet.lock() | O(1) | O(1) |
| update_mark(symbol, price) | O(1)* | O(1) | *HashMap insert/get_mut amortized O(1) |
| strategy.evaluate | aşağıda | O(1) |
| execute_buy/execute_sell | O(1)* | O(1) | *HashMap get_mut/remove amortized O(1) |

### 2.2 TrendFollowStrategy.evaluate

| İşlem | Zaman | Uzay |
|-------|--------|------|
| ensure_indicators(symbol) | O(1)* | O(1) | *HashMap lookup; ilk kez O(1) alloc |
| ema.next(price) | O(1) | O(1) | EMA(50) sabit pencere |
| rsi.next(price) | O(W) | O(1) | W=14 pencere; ta kütüphanesi genelde O(W) |
| has_position | O(1)* | O(1) |
| Karar + log | O(1) | O(1) |

**Strateji başına mesaj:** O(1) amortized (W=14 sabit). Yeni sembol: O(1) ek maliyet.

### 2.3 Wallet

| İşlem | Zaman | Uzay |
|-------|--------|------|
| update_mark | O(1)* | O(1) |
| net_worth | O(P) | O(1) | P = pozisyon sayısı (≤ S, S=50) |
| execute_buy | O(1)* | O(1) |
| execute_sell | O(1)* | O(1) |

**Kalıcı bellek:** O(P) pozisyon + O(S) latest_prices ⇒ O(S), S=50.

**Özet (sim-broker mesaj başına):** O(L) parse + O(1) hot path ⇒ **O(L)**; L genelde 200–2000.

---

## 3. market-persister

### 3.1 NATS ingest (mesaj başına)

| İşlem | Zaman | Uzay |
|-------|--------|------|
| from_slice MarketEvent | O(L) | O(L) |
| Utc::now() | O(1) | O(1) |
| to_trade_row | O(1) | O(1) | Sabit sayıda get + parse |
| to_bbo_row | O(B+A) | O(B+A) | B = bid, A = ask sayısı; best_bid O(B), best_ask O(A) |
| tx.send(row) | O(1) | O(1) | Channel buffer’a kopya |

BBO için B, A tipik 1–100; çoğu event’te <20 ⇒ **O(L) + O(B+A)** mesaj başına.

### 3.2 Writer loop (flush)

| İşlem | Zaman | Uzay |
|-------|--------|------|
| insert_trade_batch(K row) | O(K) write | O(1) | K ≤ 10_000 |
| insert_bbo_batch(K row) | O(K) write | O(1) |

Flush tetikleyen: 10_000 satır **veya** 1 saniye.

### 3.3 Bellek (persister)

| Kaynak | Boyut |
|--------|--------|
| mpsc channel | 200_000 slot × ~80 byte/row ⇒ ~16 MB |
| trade_buffer | 10_000 × ~60 byte ⇒ ~0.6 MB |
| bbo_buffer | 10_000 × ~80 byte ⇒ ~0.8 MB |

**Toplam peak:** ~18 MB (channel dolu + iki buffer dolu senaryo).

---

## 4. Özet tablo (mesaj başına, L = mesaj uzunluğu)

| Servis | Zaman (mesaj başına) | Uzay (mesaj başına) | Sabit bellek |
|--------|----------------------|----------------------|--------------|
| market-ingestor | O(L) | O(L) | O(1) |
| sim-broker | O(L) | O(L) | O(S) S=50 |
| market-persister (trade) | O(L) | O(L) | O(200k + 20k) buffer |

**Ortak:** Hepsi mesaj uzunluğu L ile lineer; ek sabitler (S, buffer boyutları) küçük.

---

## 5. Maliyet (işlem / bellek)

- **CPU (mesaj başına):** Parse O(L) baskın; L ~500 byte ortalama ⇒ ~500–2k byte işleniyor, modern CPU’da mikrosaniye mertebesi.
- **Bellek:** 
  - Ingestor: Mesaj başına geçici O(L), kalıcı O(1).
  - Sim-broker: O(S) state (S=50), mesaj başına O(L).
  - Persister: Channel + buffer ~18 MB sabit; mesaj başına O(L) geçici.
- **Ağ:** Ingestor → NATS O(L) publish; Persister → ClickHouse batch’te O(K) row, K ≤ 10k.
- **Disk (ClickHouse):** Batch insert O(K) row; partition/ORDER BY mevcut şema ile günlük partition, sorgu maliyeti sembol + zaman aralığına bağlı.

---

## 6. Darboğaz notları (değişiklik yapılmadan)

1. **market-ingestor:** Çift parse (Value + typed); L büyürse tek parse veya RawValue düşünülebilir.
2. **sim-broker:** RSI.next(14) pencere maliyeti sabit; sembol başına tek EMA+RSI, P pozisyon sayısı 50’yi geçmez.
3. **market-persister:** BBO’da bids/asks uzunluğu B+A; Binance depth @100ms genelde kısa, O(B+A) pratikte sınırlı.

Genel sonuç: Tüm pipeline mesaj başına **O(L)** zaman ve **O(L)** geçici bellek; sabit state ve buffer boyutları makul (S=50, channel 200k, buffer 10k+10k). Veri seti bütünlüğü veya davranış değiştirilmeden karmaşıklık ve maliyet bu çerçevede.

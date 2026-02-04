# Sistemi ayağa kaldırma

Proje kökünde (quantum-sentry-trader) çalıştır. Sıra önemli.

---

## 1. Altyapıyı başlat (bir kez)

```powershell
docker-compose up -d
```

- **NATS** → 4222 (client), 8222 (monitoring)
- **ClickHouse** → 8123 (HTTP), 9000 (native)
- DB adı ve kullanıcı/şifre **docker-compose** içinde tanımlı; production’da değiştirin.

Tabloları **sen oluşturmuyorsun**; market-persister ilk çalıştığında `ensure_schema()` ile oluşturur.

---

## 2. Üç servisi başlat (3 ayrı terminal)

Proje kökünden (`cargo` burada çalışacak):

**Terminal 1 – veri girişi (Binance → NATS):**
```powershell
cargo run -p market-ingestor
```

**Terminal 2 – veriyi ClickHouse’a yazma (NATS → quantum):**
```powershell
cargo run -p market-persister
```

**Terminal 3 – strateji + cüzdan simülasyonu (NATS’tan canlı):**
```powershell
cargo run -p sim-broker
```

Hepsi aynı NATS’a bağlanır; persister ClickHouse’a yazar. Durdurmak için her terminalde `Ctrl+C`.

---

## 3. (İsteğe bağlı) Ortam değişkenleri

Varsayılanlar çoğu durumda yeterli. Değiştirmek istersen:

| Değişken | Varsayılan | Açıklama |
|----------|------------|----------|
| `NATS_URL` | `nats://127.0.0.1:4222` | NATS adresi |
| `CLICKHOUSE_URL` | `http://127.0.0.1:8123` | Sadece persister |
| `CLICKHOUSE_DB` | docker-compose’taki varsayılan | Persister’ın yazdığı DB |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | docker-compose’taki varsayılan | Gerekirse ortam değişkeni ile override edin |

---

## 4. Altyapıyı durdurma

```powershell
docker-compose down
```

Veri `clickhouse_data` volume’da kalır; `docker-compose down -v` dersen volume da silinir.

---

## Özet sıra

1. `docker-compose up -d`
2. `cargo run -p market-ingestor`   (Terminal 1)
3. `cargo run -p market-persister` (Terminal 2)
4. `cargo run -p sim-broker`       (Terminal 3)

v2 (feature_engine, executor, logger) şu an çalıştırdığın 3 script’e dahil değil; ileride kullanırsan `sql/README.md` ve `configs/logger.yml`’a bak.

**Shadow-mode soak test (6–8 saat, sabah metrik):** [SOAK_TEST.md](SOAK_TEST.md) — tmux ile servis sırası, `EXECUTION_ENABLED=false`, single-writer kuralı ve sabah kontrol listesi.

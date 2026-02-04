# Shadow-mode Soak Test (6–8 saat)

**Amaç:** Sistem çökmüyor mu, drop artıyor mu, latency/spread gate sürekli mi tetikleniyor, ClickHouse’a batch yazım stabil mi? Sabah metrik bak.

**Çalıştırılacak process’ler:** market-ingestor → market-persister → feature_engine → executor (4 pencere). **Logger** ayrı binary değil; executor içinde çalışır. Logger metrikleri executor log’unda görünür.

---

## Tek kural

- **`execution_enabled=false`** kalacak. Gerçek emir gönderilmez.

## Sim-broker?

**Shadow modda zorunlu değil:** executor emir göndermediği için sim-broker’a giden emir yok. Soak test sadece pipeline stabilitesi (ingestor → persister → feature_engine → executor + logger) için 4 process yeterli.

**İstersen tam stack:** emir simülasyonu veya “tüm servisler ayakta” demek için 5. pencerede sim-broker’ı da çalıştırabilirsin: `cargo run -p sim-broker --release`.

## Single-writer kuralı (raw tick)

- **market-persister** raw tick yazıyorsa → **logger**’da raw tick logging **kapalı** olmalı (tek yazıcı).
- Örnek: `LOG_RAW_TICKS=false` (logger’da raw tablolara yazma kapalı).

---

## Ortam değişkenleri (örnek)

```bash
export EXECUTION_ENABLED=false
export LOG_RAW_TICKS=false
export FEATURE_WINDOW=60
```

İstersen `.env` veya shell/PowerShell’de set edebilirsin.

---

## Windows (PowerShell)

**tmux yok** — 4 ayrı PowerShell penceresi veya Windows Terminal sekmesi aç; proje kökünde (`quantum-sentry-trader`) çalıştır. Önce release build:

```powershell
cargo build --release
```

Ortam değişkenleri (PowerShell):

```powershell
$env:EXECUTION_ENABLED = "false"
$env:LOG_RAW_TICKS = "false"
$env:FEATURE_WINDOW = "60"
```

**1)** Altyapı: `docker compose up -d`

**2–5)** Her biri için ayrı pencere/sekme; proje kökünden. Paket adları: `market-ingestor`, `market-persister`, `feature_engine` (alt çizgi), `executor`. Logger ayrı çalıştırılmaz (executor içinde).

```powershell
# Pencere 2
cargo run -p market-ingestor --release

# Pencere 3
cargo run -p market-persister --release

# Pencere 4 (paket adı: feature_engine, alt çizgi)
cargo run -p feature_engine --release

# Pencere 5 (executor içinde logger da çalışır)
cargo run -p executor --release
```

Log dosyasına yazmak istersen (önce `mkdir logs` gerekebilir):

```powershell
cargo run -p executor --release 2>&1 | Tee-Object -FilePath logs/executor.log
```

---

## Linux / macOS / WSL (tmux)

Pencere isimleri sende farklı olabilir; sıra önemli.

```bash
tmux new -s qs
```

**1)** Altyapı (docker-compose varsa): `docker compose up -d`

**2–5)** Önce `cargo build --release` veya her komutta `cargo run -p ... --release`. Paket adı: `feature_engine` (alt çizgi). Logger ayrı çalıştırılmaz (executor içinde). Log için `tee`:

```bash
# İlk pencere
cargo run -p market-ingestor --release 2>&1 | tee logs/ingestor.log

# Yeni pencere
tmux new-window
cargo run -p market-persister --release 2>&1 | tee logs/persister.log

tmux new-window
cargo run -p feature_engine --release 2>&1 | tee logs/feature_engine.log

tmux new-window
cargo run -p executor --release 2>&1 | tee logs/executor.log
```

---

## Log nereden okunur?

- **Varsayılan:** Her process’in log’u çalıştırdığın **terminal penceresine** (stdout) yazılır. Pencere kapatılırsa log gider.
- **Dosyaya yazmak (sabah bakmak için):** Servisi başlarken çıktıyı dosyaya da ver:
  - **Windows:** Önce `New-Item -ItemType Directory -Force -Path logs`, sonra örn. `cargo run -p executor --release 2>&1 | Tee-Object -FilePath logs/executor.log`
  - **Linux/tmux:** Zaten `tee logs/executor.log` kullanıyorsan log hem ekranda hem `logs/executor.log` dosyasında.
- **Sabah:** `logs/executor.log` (ve diğer `logs/*.log`) dosyalarını açıp `flush_errors`, `events_dropped`, `latency_gate` vb. arayabilirsin. PowerShell’de: `Select-String -Path logs/executor.log -Pattern "flush_errors|events_dropped"`.

---

## Sabah bakılacaklar

| Nerede | Ne |
|--------|----|
| **executor log’u** (logger burada çalışır) | `flush_errors` var mı? |
| **executor log’u** | `events_dropped` çok yükseliyor mu? |
| **executor** | Sürekli `latency_gate` / `spread_gate` / `not_ready` dönüyor mu? |

- Logger metrikleri (flush_errors, events_dropped) executor process’inin log’unda görünür; dokümana göre kontrol et.
- Executor’da bu gate’ler sürekli tetikleniyorsa: latency hedefi aşılıyor, spread çok geniş veya feature hazır değil demektir; logları filtreleyerek say.

Soak 6–8 saat sorunsuz + drop/gate patlaması yoksa shadow pipeline stabil kabul edilebilir.

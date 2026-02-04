# sql/ klasörü ne işe yarıyor?

Bu klasördeki `.sql` dosyaları **ClickHouse tablolarının DDL referansı** (CREATE TABLE).

**Şema kaynağı (source of truth):** `rust/market-persister` içindeki **ch.rs → ensure_schema()**. sql/ dosyaları bu kodla **uyumlu** olmalıdır. Şema değişikliği yaparken **hem ensure_schema hem ilgili sql/ dosyalarını aynı PR’da güncelleyin.**

---

## 01 ve 02: trade_ticks, bbo_ticks

- **Asıl oluşturan:** `market-persister` çalışırken `ch.rs` içindeki `ensure_schema()` bu tabloları **kendisi** oluşturur (CREATE TABLE IF NOT EXISTS). PARTITION BY toDate(ts_exchange).
- **Buradaki sql’ler:** Referans / manuel kurulum (persister’ı çalıştırmadan elle tablo oluşturmak için). **01 ve 02, ensure_schema ile aynı tutulmalı.**

---

## 03, 04, 05: feature_snapshots, decisions, order_commands

- **Bunları oluşturan kod yok:** `rust/logger` (v2) bu tablolara **sadece INSERT** yapar; tabloları create etmez.
- **Ne zaman gerekir:** Feature_engine veya executor’ı **ClickHouse logger** ile çalıştıracaksan, önce bu 3 tabloyu **sen** oluşturmalısın. İşte 03, 04, 05 bu tabloların DDL’i.
- Özet: v2 pipeline (feature_engine + logger veya executor + logger) kullanacaksan **03, 04, 05’i bir kez çalıştırman gerekir**; yoksa logger INSERT atarken “table not found” alırsın.

---

## Kısa tablo

| Dosya        | Tablo              | Kim kullanır / oluşturur                          | Zorunlu mu?                          |
|-------------|--------------------|---------------------------------------------------|--------------------------------------|
| 01          | trade_ticks        | Persister (ch.rs) oluşturur; bu dosya referans   | Hayır (persister kendi yaratıyor)   |
| 02          | bbo_ticks          | Aynı                                              | Hayır                                |
| 03          | feature_snapshots  | Logger yazar; tabloyu sen 03 ile oluşturursun    | Evet, v2 logger kullanıyorsan       |
| 04          | decisions          | Aynı                                              | Evet, v2 logger kullanıyorsan       |
| 05          | order_commands     | Aynı                                              | Evet, v2 logger kullanıyorsan       |

**Sadece 3 script (ingestor, persister, sim-broker) çalışıyorsan:** 01 ve 02’yi çalıştırmana gerek yok; 03, 04, 05 hiç gerekmez.

**v2 (feature_engine/executor + ClickHouse logger) kullanacaksan:** Önce `03_feature_snapshots.sql`, `04_decisions.sql`, `05_order_commands.sql` dosyalarını ClickHouse’ta çalıştır (quantum veritabanında).

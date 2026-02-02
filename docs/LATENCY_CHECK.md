# Depth latency kontrolü (~1117 ms)

## Yazılımsal çözüm (uygulandı)

**market-ingestor** artık Binance Futures server time API (`GET /fapi/v1/time`) ile saat farkını düzeltiyor:

- Başlangıçta ve her 30 saniyede bir Binance server time alınıyor.
- `clock_offset_ms = local_now - server_time` hesaplanıp `Arc<AtomicI64>` ile tutuluyor.
- `ts_ingest = now_ms() - clock_offset_ms` ile ts_ingest **Binance zamanına** normalize ediliyor; böylece `dateDiff(ts_exchange, ts_ingest)` gerçek pipeline gecikmesini gösterir (saat farkı etkisiz).

Ingestor’u yeniden başlattıktan sonra ~30 sn içinde offset güncellenir; sonraki depth/context mesajları düzeltilmiş ts_ingest ile gider. Eski satırlar eski (düzeltilmemiş) ts_ingest ile kalır.

## Sorgular (ClickHouse)

Ortalama gecikme (son 1 dk):
```bash
docker exec clickhouse-server clickhouse-client -q "SELECT avg(dateDiff('millisecond', ts_exchange, ts_ingest)) AS avg_latency_ms FROM quantum.depth_snapshots WHERE ts_exchange > now() - INTERVAL 1 MINUTE"
```

Dağılım (sabit mi = saat farkı, değişken mi = ağ):
```bash
docker exec clickhouse-server clickhouse-client -q "SELECT quantile(0.5)(dateDiff('millisecond', ts_exchange, ts_ingest)) AS p50_ms, quantile(0.99)(dateDiff('millisecond', ts_exchange, ts_ingest)) AS p99_ms, stddevPop(dateDiff('millisecond', ts_exchange, ts_ingest)) AS stddev_ms FROM quantum.depth_snapshots WHERE ts_exchange > now() - INTERVAL 1 MINUTE"
```

Min/Max (saat farkı varsa hep benzer çıkar):
```bash
docker exec clickhouse-server clickhouse-client -q "SELECT min(dateDiff('millisecond', ts_exchange, ts_ingest)) AS min_ms, max(dateDiff('millisecond', ts_exchange, ts_ingest)) AS max_ms FROM quantum.depth_snapshots WHERE ts_exchange > now() - INTERVAL 1 MINUTE"
```

## Ne yapmalı?

1. **Saat senkronu**: Makinede NTP açık olsun (Windows: "Set time automatically" açık; sunucuda `chronyd`/`ntpd`).
2. **Timezone**: Hem Binance "E" hem bizim `now()` UTC ise fark olmaz; karışık timezone varsa offset çıkar.
3. **Persister tarafında ts_ingest**: İstersen persister NATS’tan mesajı aldığı anda bir `ts_ingest_persister` yazıp tam pipeline gecikmesini (exchange → persister) ölçebilirsin; mevcut metrik sadece exchange → ingestor.

Özet: **~1.1 s HFT için yüksek; önce saat farkını (NTP + timezone) kontrol et, sonra yukarıdaki dağılım sorgularıyla sabit mi değişken mi bak.**

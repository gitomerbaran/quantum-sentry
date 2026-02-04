mod ch;
mod model;

use crate::ch::ClickHouseWriter;
use crate::model::{
    BBOTickRow, DepthSnapshot, DepthSnapshotRow, LiquidationTick, LiquidationTickRow,
    MarketContextEvent, MarketEvent, TradeTickRow,
};

use chrono::Utc;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{debug, error, info, warn};

#[derive(Debug)]
pub enum MarketRow {
    Trade(TradeTickRow),
    BBO(BBOTickRow),
}

#[tokio::main]
async fn main() {
    init_tracing();

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let subject_raw = std::env::var("NATS_SUBJECT").unwrap_or_else(|_| "market.raw".to_string());
    let subject_context =
        std::env::var("NATS_SUBJECT_CONTEXT").unwrap_or_else(|_| "market.context".to_string());

    let (tx_raw, rx_raw) = mpsc::channel::<MarketRow>(200_000);
    let (tx_context, rx_context) = mpsc::channel::<crate::model::MarketContextRow>(200_000);
    let (tx_depth, rx_depth) = mpsc::channel::<DepthSnapshotRow>(200_000);
    let (tx_liq, rx_liq) = mpsc::channel::<LiquidationTickRow>(50_000);

    let nats_raw_handle = tokio::spawn(nats_ingest_raw_loop(nats_url.clone(), subject_raw, tx_raw));
    let nats_context_handle = tokio::spawn(nats_ingest_context_loop(
        nats_url.clone(),
        subject_context,
        tx_context,
    ));
    let nats_depth_handle = tokio::spawn(nats_ingest_depth_loop(nats_url.clone(), tx_depth));
    let nats_liq_handle = tokio::spawn(nats_ingest_liq_loop(nats_url, tx_liq));

    let ch_handle = tokio::spawn(clickhouse_writer_loop(rx_raw));
    let ch_context_handle = tokio::spawn(clickhouse_writer_context_loop(rx_context));
    let ch_depth_handle = tokio::spawn(depth_writer_loop(rx_depth));
    let ch_liq_handle = tokio::spawn(liq_writer_loop(rx_liq));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C received; shutting down market-persister...");
        }
        _ = nats_raw_handle => {
            warn!("NATS raw task ended unexpectedly; shutting down...");
        }
        _ = nats_context_handle => {
            warn!("NATS context task ended unexpectedly; shutting down...");
        }
        _ = nats_depth_handle => {
            warn!("NATS depth task ended unexpectedly; shutting down...");
        }
        _ = nats_liq_handle => {
            warn!("NATS liq task ended unexpectedly; shutting down...");
        }
        _ = ch_handle => {
            warn!("ClickHouse writer task ended unexpectedly; shutting down...");
        }
        _ = ch_context_handle => {
            warn!("ClickHouse context writer task ended unexpectedly; shutting down...");
        }
        _ = ch_depth_handle => {
            warn!("ClickHouse depth writer task ended unexpectedly; shutting down...");
        }
        _ = ch_liq_handle => {
            warn!("ClickHouse liq writer task ended unexpectedly; shutting down...");
        }
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

/// Ingest market.raw: Case A = trade (p,q,t), Case B = BBO (b,a,u). ts_ingest captured at parse time.
async fn nats_ingest_raw_loop(nats_url: String, subject: String, tx: mpsc::Sender<MarketRow>) {
    let mut backoff_ms: u64 = 250;
    let backoff_max_ms: u64 = 5_000;

    loop {
        info!("Connecting to NATS at {nats_url}...");
        match async_nats::connect(&nats_url).await {
            Ok(client) => {
                info!("Connected to NATS. Subscribing to {subject}...");
                match client.subscribe(subject.clone()).await {
                    Ok(mut sub) => {
                        info!("Subscribed. Ingesting messages (trade + BBO split)...");
                        backoff_ms = 250;

                        while let Some(msg) = sub.next().await {
                            let evt: Result<MarketEvent, _> = serde_json::from_slice(&msg.payload);
                            match evt {
                                Ok(e) => {
                                    // Capture ingest time (UTC) immediately upon parse for latency tracking.
                                    let ts_ingest_ms = Utc::now().timestamp_millis();

                                    if let Some(row) = e.to_trade_row(ts_ingest_ms) {
                                        if let Err(e) = tx.send(MarketRow::Trade(row)).await {
                                            warn!(
                                                "Writer channel closed; stopping NATS ingest: {e}"
                                            );
                                            return;
                                        }
                                        continue;
                                    }
                                    if let Some(row) = e.to_bbo_row(ts_ingest_ms) {
                                        if let Err(e) = tx.send(MarketRow::BBO(row)).await {
                                            warn!(
                                                "Writer channel closed; stopping NATS ingest: {e}"
                                            );
                                            return;
                                        }
                                        continue;
                                    }
                                    debug!("Skipping event (no valid trade or BBO)");
                                }
                                Err(e) => {
                                    warn!("Failed to deserialize MarketEvent JSON (skipping): {e}");
                                }
                            }
                        }

                        warn!("NATS subscription ended (connection drop?). Will reconnect.");
                    }
                    Err(e) => {
                        error!("NATS subscribe error: {e}");
                    }
                }
            }
            Err(e) => {
                error!("NATS connect error: {e}");
            }
        }

        warn!("Retrying NATS connect in {backoff_ms}ms...");
        time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(backoff_max_ms);
    }
}

/// Ingest market.context (Futures: mark price, funding rate, open interest). Deserialize -> to_row -> send.
async fn nats_ingest_context_loop(
    nats_url: String,
    subject: String,
    tx: mpsc::Sender<crate::model::MarketContextRow>,
) {
    let mut backoff_ms: u64 = 250;
    let backoff_max_ms: u64 = 5_000;

    loop {
        info!("Connecting to NATS at {nats_url} (context)...");
        match async_nats::connect(&nats_url).await {
            Ok(client) => {
                info!("Connected to NATS. Subscribing to {subject}...");
                match client.subscribe(subject.clone()).await {
                    Ok(mut sub) => {
                        info!("Subscribed to market.context. Ingesting...");
                        backoff_ms = 250;

                        while let Some(msg) = sub.next().await {
                            let evt: Result<MarketContextEvent, _> =
                                serde_json::from_slice(&msg.payload);
                            match evt {
                                Ok(e) => {
                                    let row = e.to_row();
                                    if let Err(e) = tx.send(row).await {
                                        warn!("Context writer channel closed; stopping: {e}");
                                        return;
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to deserialize MarketContextEvent (skipping): {e}"
                                    );
                                }
                            }
                        }

                        warn!("NATS context subscription ended; reconnecting.");
                    }
                    Err(e) => {
                        error!("NATS context subscribe error: {e}");
                    }
                }
            }
            Err(e) => {
                error!("NATS connect error (context): {e}");
            }
        }

        warn!("Retrying NATS connect in {backoff_ms}ms...");
        time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(backoff_max_ms);
    }
}

/// Ingest market.depth.* (Futures L2 depth snapshots). Deserialize -> to_row -> send.
async fn nats_ingest_depth_loop(nats_url: String, tx: mpsc::Sender<DepthSnapshotRow>) {
    let mut backoff_ms: u64 = 250;
    let backoff_max_ms: u64 = 5_000;
    let subject = "market.depth.*";

    loop {
        info!("Connecting to NATS at {nats_url} (depth)...");
        match async_nats::connect(&nats_url).await {
            Ok(client) => {
                info!("Connected to NATS. Subscribing to {subject}...");
                match client.subscribe(subject.to_string()).await {
                    Ok(mut sub) => {
                        info!("Subscribed to market.depth.*. Ingesting...");
                        backoff_ms = 250;

                        while let Some(msg) = sub.next().await {
                            let snap: Result<DepthSnapshot, _> =
                                serde_json::from_slice(&msg.payload);
                            match snap {
                                Ok(s) => {
                                    let row = s.to_row();
                                    if let Err(e) = tx.send(row).await {
                                        error!("Depth channel send failed: {e}");
                                        return;
                                    }
                                }
                                Err(e) => {
                                    warn!("Depth JSON parse error (skipping): {e}");
                                }
                            }
                        }

                        warn!("NATS depth subscription ended; reconnecting.");
                    }
                    Err(e) => {
                        error!("NATS depth subscribe error: {e}");
                    }
                }
            }
            Err(e) => {
                error!("NATS connect error (depth): {e}");
            }
        }

        warn!("Retrying NATS connect in {backoff_ms}ms...");
        time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(backoff_max_ms);
    }
}

/// Ingest market.liq.* (Futures liquidation events). Deserialize -> to_row -> send.
async fn nats_ingest_liq_loop(nats_url: String, tx: mpsc::Sender<LiquidationTickRow>) {
    let mut backoff_ms: u64 = 250;
    let backoff_max_ms: u64 = 5_000;
    let subject = "market.liq.*";

    loop {
        info!("Connecting to NATS at {nats_url} (liq)...");
        match async_nats::connect(&nats_url).await {
            Ok(client) => {
                info!("Connected to NATS. Subscribing to {subject}...");
                match client.subscribe(subject.to_string()).await {
                    Ok(mut sub) => {
                        info!("Subscribed to market.liq.*. Ingesting...");
                        backoff_ms = 250;

                        while let Some(msg) = sub.next().await {
                            let liq: Result<LiquidationTick, _> =
                                serde_json::from_slice(&msg.payload);
                            match liq {
                                Ok(l) => {
                                    let row = l.to_row();
                                    if let Err(e) = tx.send(row).await {
                                        error!("Liq channel send failed: {e}");
                                        return;
                                    }
                                }
                                Err(e) => {
                                    warn!("Liq JSON parse error (skipping): {e}");
                                }
                            }
                        }

                        warn!("NATS liq subscription ended; reconnecting.");
                    }
                    Err(e) => {
                        error!("NATS liq subscribe error: {e}");
                    }
                }
            }
            Err(e) => {
                error!("NATS connect error (liq): {e}");
            }
        }

        warn!("Retrying NATS connect in {backoff_ms}ms...");
        time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(backoff_max_ms);
    }
}

async fn clickhouse_writer_loop(mut rx: mpsc::Receiver<MarketRow>) {
    let mut writer = ClickHouseWriter::new_from_env();

    let mut backoff_ms: u64 = 250;
    let backoff_max_ms: u64 = 5_000;

    loop {
        match writer.ping().await {
            Ok(_) => break,
            Err(e) => {
                warn!("ClickHouse not ready yet: {e}. retry in {backoff_ms}ms");
                time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(backoff_max_ms);
                writer = ClickHouseWriter::new_from_env();
            }
        }
    }

    if let Err(e) = writer.ensure_schema().await {
        error!("Failed to ensure ClickHouse schema: {e}");
    }

    let mut trade_buffer: Vec<TradeTickRow> = Vec::with_capacity(10_000);
    let mut bbo_buffer: Vec<BBOTickRow> = Vec::with_capacity(10_000);
    let mut tick = time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = tick.tick() => {
                if !trade_buffer.is_empty() {
                    if let Err(e) = flush_trade_with_retry(&mut writer, &mut trade_buffer).await {
                        warn!("Trade flush failed; buffer retained (len={}): {e}", trade_buffer.len());
                    }
                }
                if !bbo_buffer.is_empty() {
                    if let Err(e) = flush_bbo_with_retry(&mut writer, &mut bbo_buffer).await {
                        warn!("BBO flush failed; buffer retained (len={}): {e}", bbo_buffer.len());
                    }
                }
            }

            maybe_row = rx.recv() => {
                match maybe_row {
                    Some(MarketRow::Trade(row)) => {
                        trade_buffer.push(row);
                        if trade_buffer.len() >= 10_000 {
                            if let Err(e) = flush_trade_with_retry(&mut writer, &mut trade_buffer).await {
                                warn!("Trade flush failed; buffer retained (len={}): {e}", trade_buffer.len());
                            }
                        }
                    }
                    Some(MarketRow::BBO(row)) => {
                        bbo_buffer.push(row);
                        if bbo_buffer.len() >= 10_000 {
                            if let Err(e) = flush_bbo_with_retry(&mut writer, &mut bbo_buffer).await {
                                warn!("BBO flush failed; buffer retained (len={}): {e}", bbo_buffer.len());
                            }
                        }
                    }
                    None => {
                        warn!("Writer channel closed; exiting ClickHouse writer loop.");
                        if !trade_buffer.is_empty() {
                            let _ = flush_trade_with_retry(&mut writer, &mut trade_buffer).await;
                        }
                        if !bbo_buffer.is_empty() {
                            let _ = flush_bbo_with_retry(&mut writer, &mut bbo_buffer).await;
                        }
                        return;
                    }
                }
            }
        }
    }
}

/// Writer loop for market_context: buffer 10k rows OR 1s, then flush. On failure: retain buffer, recreate client, retry.
async fn clickhouse_writer_context_loop(mut rx: mpsc::Receiver<crate::model::MarketContextRow>) {
    let mut writer = ClickHouseWriter::new_from_env();

    let mut backoff_ms: u64 = 250;
    let backoff_max_ms: u64 = 5_000;

    loop {
        match writer.ping().await {
            Ok(_) => break,
            Err(e) => {
                warn!("ClickHouse not ready yet (context): {e}. retry in {backoff_ms}ms");
                time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(backoff_max_ms);
                writer = ClickHouseWriter::new_from_env();
            }
        }
    }

    if let Err(e) = writer.ensure_schema().await {
        error!("Failed to ensure ClickHouse schema (context): {e}");
    }

    let mut buffer: Vec<crate::model::MarketContextRow> = Vec::with_capacity(10_000);
    let mut tick = time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = tick.tick() => {
                if !buffer.is_empty() {
                    if let Err(e) = flush_context_with_retry(&mut writer, &mut buffer).await {
                        warn!("Context flush failed; buffer retained (len={}): {e}", buffer.len());
                    }
                }
            }

            maybe_row = rx.recv() => {
                match maybe_row {
                    Some(row) => {
                        buffer.push(row);
                        if buffer.len() >= 10_000 {
                            if let Err(e) = flush_context_with_retry(&mut writer, &mut buffer).await {
                                warn!("Context flush failed; buffer retained (len={}): {e}", buffer.len());
                            }
                        }
                    }
                    None => {
                        warn!("Context writer channel closed; exiting.");
                        if !buffer.is_empty() {
                            let _ = flush_context_with_retry(&mut writer, &mut buffer).await;
                        }
                        return;
                    }
                }
            }
        }
    }
}

async fn flush_context_with_retry(
    writer: &mut ClickHouseWriter,
    buffer: &mut Vec<crate::model::MarketContextRow>,
) -> Result<(), clickhouse::error::Error> {
    match writer.insert_context_batch(buffer).await {
        Ok(_) => {
            info!("Flushed {} context rows into ClickHouse", buffer.len());
            buffer.clear();
            Ok(())
        }
        Err(e) => {
            warn!("ClickHouse context insert error: {e}. Buffer retained.");
            tokio::time::sleep(Duration::from_millis(500)).await;
            *writer = ClickHouseWriter::new_from_env();
            let _ = writer.ping().await;
            let _ = writer.ensure_schema().await;
            Err(e)
        }
    }
}

async fn flush_trade_with_retry(
    writer: &mut ClickHouseWriter,
    buffer: &mut Vec<TradeTickRow>,
) -> Result<(), clickhouse::error::Error> {
    match writer.insert_trade_batch(buffer).await {
        Ok(_) => {
            info!("Flushed {} trade rows into ClickHouse", buffer.len());
            buffer.clear();
            Ok(())
        }
        Err(e) => {
            warn!("ClickHouse trade insert error: {e}. Buffer retained.");
            tokio::time::sleep(Duration::from_millis(500)).await;
            *writer = ClickHouseWriter::new_from_env();
            let _ = writer.ping().await;
            let _ = writer.ensure_schema().await;
            Err(e)
        }
    }
}

async fn flush_bbo_with_retry(
    writer: &mut ClickHouseWriter,
    buffer: &mut Vec<BBOTickRow>,
) -> Result<(), clickhouse::error::Error> {
    match writer.insert_bbo_batch(buffer).await {
        Ok(_) => {
            info!("Flushed {} BBO rows into ClickHouse", buffer.len());
            buffer.clear();
            Ok(())
        }
        Err(e) => {
            warn!("ClickHouse BBO insert error: {e}. Buffer retained.");
            tokio::time::sleep(Duration::from_millis(500)).await;
            *writer = ClickHouseWriter::new_from_env();
            let _ = writer.ping().await;
            let _ = writer.ensure_schema().await;
            Err(e)
        }
    }
}

/// Writer loop for depth_snapshots: flush when buffer >= 1000 or every 1s. On failure: retain buffer, retry.
const DEPTH_BATCH_SIZE: usize = 1000;

async fn depth_writer_loop(mut rx: mpsc::Receiver<DepthSnapshotRow>) {
    let mut writer = ClickHouseWriter::new_from_env();
    let mut backoff_ms: u64 = 250;
    let backoff_max_ms: u64 = 5_000;

    loop {
        match writer.ping().await {
            Ok(_) => break,
            Err(e) => {
                warn!("ClickHouse not ready yet (depth): {e}. retry in {backoff_ms}ms");
                time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(backoff_max_ms);
                writer = ClickHouseWriter::new_from_env();
            }
        }
    }

    if let Err(e) = writer.ensure_schema().await {
        error!("Failed to ensure ClickHouse schema (depth): {e}");
    }

    let mut buf: Vec<DepthSnapshotRow> = Vec::with_capacity(2000);
    let mut tick = time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = tick.tick() => {
                if !buf.is_empty() {
                    if let Err(e) = flush_depth_with_retry(&mut writer, &mut buf).await {
                        warn!("Depth flush failed; buffer retained (len={}): {e}", buf.len());
                    }
                }
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(row) => {
                        buf.push(row);
                        if buf.len() >= DEPTH_BATCH_SIZE {
                            if let Err(e) = flush_depth_with_retry(&mut writer, &mut buf).await {
                                warn!("Depth flush failed; buffer retained (len={}): {e}", buf.len());
                            }
                        }
                    }
                    None => {
                        warn!("Depth writer channel closed; exiting.");
                        if !buf.is_empty() {
                            let _ = flush_depth_with_retry(&mut writer, &mut buf).await;
                        }
                        return;
                    }
                }
            }
        }
    }
}

async fn flush_depth_with_retry(
    writer: &mut ClickHouseWriter,
    buffer: &mut Vec<DepthSnapshotRow>,
) -> Result<(), clickhouse::error::Error> {
    match writer.insert_depth_batch(buffer).await {
        Ok(_) => {
            info!("Flushed {} depth rows into ClickHouse", buffer.len());
            buffer.clear();
            Ok(())
        }
        Err(e) => {
            warn!("ClickHouse depth insert error: {e}. Buffer retained.");
            tokio::time::sleep(Duration::from_millis(500)).await;
            *writer = ClickHouseWriter::new_from_env();
            let _ = writer.ping().await;
            let _ = writer.ensure_schema().await;
            Err(e)
        }
    }
}

/// Writer loop for liquidation_ticks: flush when buffer >= 1000 or every 1s. On failure: retain buffer, retry.
const LIQ_BATCH_SIZE: usize = 1000;

async fn liq_writer_loop(mut rx: mpsc::Receiver<LiquidationTickRow>) {
    let mut writer = ClickHouseWriter::new_from_env();
    let mut backoff_ms: u64 = 250;
    let backoff_max_ms: u64 = 5_000;

    loop {
        match writer.ping().await {
            Ok(_) => break,
            Err(e) => {
                warn!("ClickHouse not ready yet (liq): {e}. retry in {backoff_ms}ms");
                time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(backoff_max_ms);
                writer = ClickHouseWriter::new_from_env();
            }
        }
    }

    if let Err(e) = writer.ensure_schema().await {
        error!("Failed to ensure ClickHouse schema (liq): {e}");
    }

    let mut buf: Vec<LiquidationTickRow> = Vec::with_capacity(2000);
    let mut tick = time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = tick.tick() => {
                if !buf.is_empty() {
                    if let Err(e) = flush_liq_with_retry(&mut writer, &mut buf).await {
                        warn!("Liq flush failed; buffer retained (len={}): {e}", buf.len());
                    }
                }
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(row) => {
                        buf.push(row);
                        if buf.len() >= LIQ_BATCH_SIZE {
                            if let Err(e) = flush_liq_with_retry(&mut writer, &mut buf).await {
                                warn!("Liq flush failed; buffer retained (len={}): {e}", buf.len());
                            }
                        }
                    }
                    None => {
                        warn!("Liq writer channel closed; exiting.");
                        if !buf.is_empty() {
                            let _ = flush_liq_with_retry(&mut writer, &mut buf).await;
                        }
                        return;
                    }
                }
            }
        }
    }
}

async fn flush_liq_with_retry(
    writer: &mut ClickHouseWriter,
    buffer: &mut Vec<LiquidationTickRow>,
) -> Result<(), clickhouse::error::Error> {
    match writer.insert_liq_batch(buffer).await {
        Ok(_) => {
            info!("Flushed {} liq rows into ClickHouse", buffer.len());
            buffer.clear();
            Ok(())
        }
        Err(e) => {
            warn!("ClickHouse liq insert error: {e}. Buffer retained.");
            tokio::time::sleep(Duration::from_millis(500)).await;
            *writer = ClickHouseWriter::new_from_env();
            let _ = writer.ping().await;
            let _ = writer.ensure_schema().await;
            Err(e)
        }
    }
}

use anyhow::{Context, Result};
use reqwest::Client;

use crate::model::Ticker24hr;

const BINANCE_REST_BASE: &str = "https://api.binance.com";
const TICKER_24HR_PATH: &str = "/api/v3/ticker/24hr";

/// Fetch the top 50 USDT-quoted symbols by 24h quote volume.
///
/// Selection:
/// - We rank by `quoteVolume` (descending) among symbols ending in "USDT".
/// - After choosing the top 50, we sort symbols alphabetically before returning
///   to stabilize the combined-stream URL (avoids reconnect churn due to rank re-ordering).
pub async fn fetch_top_50_coins(client: &Client) -> Result<Vec<String>> {
    let url = format!("{BINANCE_REST_BASE}{TICKER_24HR_PATH}");

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("failed to send request to {url}"))?
        .error_for_status()
        .with_context(|| format!("non-success HTTP status from {url}"))?;

    let tickers: Vec<Ticker24hr> = resp
        .json()
        .await
        .context("failed to deserialize /api/v3/ticker/24hr response")?;

    // Filter to USDT pairs and rank by quote volume (quote asset volume).
    let mut ranked: Vec<(f64, String)> = Vec::with_capacity(tickers.len());

    for t in tickers {
        if !t.symbol.ends_with("USDT") {
            continue;
        }

        let vol = t.quote_volume.parse::<f64>().with_context(|| {
            format!(
                "failed to parse quoteVolume '{}' for symbol {}",
                t.quote_volume, t.symbol
            )
        })?;

        ranked.push((vol, t.symbol));
    }

    // Sort by quote volume descending. Use match to avoid unwrap in production.
    ranked.sort_by(|a, b| match b.0.partial_cmp(&a.0) {
        Some(o) => o,
        None => std::cmp::Ordering::Equal,
    });

    // Take top 50 and normalize to lowercase.
    let mut symbols: Vec<String> = ranked
        .into_iter()
        .take(50)
        .map(|(_, sym)| sym.to_lowercase())
        .collect();

    // Stabilize ordering for the stream URL: alphabetical sort => stable combined
    // stream URL => fewer reconnects when only rank order changes, not membership.
    symbols.sort();
    symbols.dedup();

    Ok(symbols)
}

#[cfg(test)]
mod tests {
    #[test]
    fn usdt_filtering_and_sorting_is_reasonable() {
        // This test is network-free and only validates our selection/normalization expectations
        // using fake data.
        let mut ranked: Vec<(f64, String)> = vec![
            (10.0, "BTCUSDT".to_string()),
            (5.0, "ETHUSDT".to_string()),
            (999.0, "FOOUSD".to_string()), // should be excluded (not USDT suffix)
            (7.0, "BNBUSDT".to_string()),
        ];

        ranked.retain(|(_, s)| s.ends_with("USDT"));
        ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        let mut symbols: Vec<String> = ranked
            .into_iter()
            .take(50)
            .map(|(_, s)| s.to_lowercase())
            .collect();
        symbols.sort();
        symbols.dedup();

        assert_eq!(symbols, vec!["bnbusdt", "btcusdt", "ethusdt"]);
    }
}

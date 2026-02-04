//! Strategy trait and implementations; evaluate receives market event, wallet, and symbol context (mark/funding/OI).

use std::collections::HashMap;

use common::Action as ContractAction;
use ta::indicators::{ExponentialMovingAverage as Ema, RelativeStrengthIndex as Rsi};
use ta::Next;
use tracing::{debug, info, warn};

use crate::model::MarketEvent;
use crate::wallet::Wallet;

/// Map strategy TradeAction to shared contract Action (minimal adoption of rust/common).
fn trade_action_to_contract_action(ta: &TradeAction) -> ContractAction {
    match ta {
        TradeAction::OpenLong { .. } => ContractAction::Long,
        TradeAction::OpenShort { .. } => ContractAction::Short,
        TradeAction::ClosePosition | TradeAction::Hold => ContractAction::NoTrade,
    }
}

/// Context for a symbol from market.context: mark price, funding rate, open interest.
/// Passed to Strategy::evaluate; strategies may use these for signals (e.g. funding-aware).
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct SymbolContext {
    pub mark_price: Option<f64>,
    pub funding_rate: Option<f64>,
    pub open_interest: Option<f64>,
    pub next_funding_time: Option<i64>,
}

/// Build SymbolContext from wallet state for a given symbol.
pub fn symbol_context_from_wallet(wallet: &Wallet, symbol: &str) -> SymbolContext {
    let symbol = crate::model::norm_symbol(symbol);
    SymbolContext {
        mark_price: wallet.mark_prices.get(&symbol).copied(),
        funding_rate: wallet.funding.get(&symbol).map(|f| f.funding_rate),
        open_interest: wallet.open_interest.get(&symbol).copied(),
        next_funding_time: wallet.funding.get(&symbol).map(|f| f.next_funding_time),
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TradeAction {
    OpenLong {
        usdt_margin: f64,
        leverage: u8,
    },
    #[allow(dead_code)]
    OpenShort {
        usdt_margin: f64,
        leverage: u8,
    },
    ClosePosition,
    Hold,
}

pub trait Strategy: Send {
    fn evaluate(
        &mut self,
        market_event: &MarketEvent,
        wallet: &Wallet,
        ctx: &SymbolContext,
    ) -> TradeAction;
}

/// Trend-follow: EMA(50) + RSI(14). Open long on pullback in uptrend, close on overbought.
pub struct TrendFollowStrategy {
    pub indicators: HashMap<String, (Ema, Rsi)>,
}

impl TrendFollowStrategy {
    pub fn new() -> Self {
        Self {
            indicators: HashMap::new(),
        }
    }

    fn ensure_indicators(&mut self, symbol: &str) -> Option<&mut (Ema, Rsi)> {
        if !self.indicators.contains_key(symbol) {
            let ema = match Ema::new(50) {
                Ok(v) => v,
                Err(e) => {
                    warn!(symbol = symbol, error = %e, "failed to create EMA(50)");
                    return None;
                }
            };
            let rsi = match Rsi::new(14) {
                Ok(v) => v,
                Err(e) => {
                    warn!(symbol = symbol, error = %e, "failed to create RSI(14)");
                    return None;
                }
            };
            self.indicators.insert(symbol.to_string(), (ema, rsi));
        }
        self.indicators.get_mut(symbol)
    }
}

impl Strategy for TrendFollowStrategy {
    fn evaluate(
        &mut self,
        market_event: &MarketEvent,
        wallet: &Wallet,
        _ctx: &SymbolContext,
    ) -> TradeAction {
        let symbol = match market_event.symbol() {
            Some(s) => s,
            None => return TradeAction::Hold,
        };

        let price = match market_event.price_f64() {
            Some(p) if p > 0.0 => p,
            _ => return TradeAction::Hold,
        };

        let (ema, rsi) = match self.ensure_indicators(&symbol) {
            Some(v) => v,
            None => return TradeAction::Hold,
        };

        let ema_val = ema.next(price);
        let rsi_val = rsi.next(price);
        let has_pos = wallet.has_position(&symbol);

        let action = if !has_pos && price > ema_val && rsi_val < 45.0 {
            TradeAction::OpenLong {
                usdt_margin: 100.0,
                leverage: 1,
            }
        } else if has_pos && rsi_val > 70.0 {
            TradeAction::ClosePosition
        } else {
            TradeAction::Hold
        };

        match action {
            TradeAction::OpenLong { .. } => {
                info!(
                    symbol = %symbol,
                    price = price,
                    ema = ema_val,
                    rsi = rsi_val,
                    contract_action = ?trade_action_to_contract_action(&action),
                    "signal -> OPEN LONG"
                );
            }
            TradeAction::OpenShort { .. } => {
                info!(
                    symbol = %symbol,
                    price = price,
                    ema = ema_val,
                    rsi = rsi_val,
                    "signal -> OPEN SHORT"
                );
            }
            TradeAction::ClosePosition => {
                info!(
                    symbol = %symbol,
                    price = price,
                    ema = ema_val,
                    rsi = rsi_val,
                    "signal -> CLOSE"
                );
            }
            TradeAction::Hold => {
                debug!(
                    symbol = %symbol,
                    price = price,
                    ema = ema_val,
                    rsi = rsi_val,
                    "signal -> HOLD"
                );
            }
        }

        action
    }
}

/// Ping-pong: open long when no position, close when price above entry threshold.
#[allow(dead_code)]
pub struct PingPongStrategy;

impl Strategy for PingPongStrategy {
    fn evaluate(
        &mut self,
        market_event: &MarketEvent,
        wallet: &Wallet,
        _ctx: &SymbolContext,
    ) -> TradeAction {
        let symbol = match market_event.symbol() {
            Some(s) => s,
            None => return TradeAction::Hold,
        };

        let price = match market_event.price_f64() {
            Some(p) => p,
            None => return TradeAction::Hold,
        };

        if wallet.usdt_balance < 10.0 {
            return TradeAction::Hold;
        }

        if !wallet.has_position(&symbol) {
            return TradeAction::OpenLong {
                usdt_margin: 100.0,
                leverage: 1,
            };
        }

        let entry = match wallet.position_entry_price(&symbol) {
            Some(p) => p,
            None => return TradeAction::Hold,
        };

        if price > entry * 1.005 {
            TradeAction::ClosePosition
        } else {
            TradeAction::Hold
        }
    }
}

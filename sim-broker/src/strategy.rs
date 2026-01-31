use crate::model::MarketEvent;
use crate::wallet::Wallet;

#[derive(Debug, Clone, Copy)]
pub enum TradeAction {
    Buy(f64),
    Sell,
    Hold,
}

pub trait Strategy {
    fn evaluate(&self, market_data: &MarketEvent, wallet: &Wallet) -> TradeAction;
}

pub struct PingPongStrategy;

impl Strategy for PingPongStrategy {
    fn evaluate(&self, market_data: &MarketEvent, wallet: &Wallet) -> TradeAction {
        let symbol = match market_data.symbol() {
            Some(s) => s,
            None => return TradeAction::Hold,
        };

        let price = match market_data.price_f64() {
            Some(p) => p,
            None => return TradeAction::Hold,
        };

        if wallet.usdt_balance < 10.0 {
            return TradeAction::Hold;
        }

        if !wallet.has_position(&symbol) {
            return TradeAction::Buy(100.0);
        }

        let entry = match wallet.position_entry_price(&symbol) {
            Some(p) => p,
            None => return TradeAction::Hold,
        };

        if price > entry * 1.005 {
            TradeAction::Sell
        } else {
            TradeAction::Hold
        }
    }
}

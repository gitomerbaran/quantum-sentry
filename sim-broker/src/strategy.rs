use crate::wallet::Wallet;

#[derive(Debug, Clone, Copy)]
pub enum Action {
    Buy(f64),
    Sell,
    Hold,
}

/// Ping-pong test strategy:
/// - If no position: buy 100 USDT worth.
/// - If in position and price > entry * 1.005: sell.
/// - Else hold.
pub fn evaluate(wallet: &Wallet, symbol: &str, price: f64) -> Action {
    if wallet.usdt_balance < 10.0 {
        return Action::Hold;
    }

    if !wallet.has_position(symbol) {
        return Action::Buy(100.0);
    }

    let entry = match wallet.position_entry_price(symbol) {
        Some(p) => p,
        None => return Action::Hold,
    };

    if price > entry * 1.005 {
        Action::Sell
    } else {
        Action::Hold
    }
}

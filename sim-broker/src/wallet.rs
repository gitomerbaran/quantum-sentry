use rand::Rng;
use std::collections::HashMap;
use tracing::info;

#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: String,
    pub amount: f64,
    pub average_entry_price: f64,
    pub current_value: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug)]
pub struct Wallet {
    pub usdt_balance: f64,
    pub positions: HashMap<String, Position>,
    pub latest_prices: HashMap<String, f64>,
}

impl Wallet {
    pub fn new() -> Self {
        Self {
            usdt_balance: 1000.0,
            positions: HashMap::new(),
            latest_prices: HashMap::new(),
        }
    }

    pub fn has_position(&self, symbol: &str) -> bool {
        self.positions.contains_key(symbol)
    }

    pub fn position_entry_price(&self, symbol: &str) -> Option<f64> {
        self.positions.get(symbol).map(|p| p.average_entry_price)
    }

    pub fn update_mark(&mut self, symbol: &str, price: f64) {
        self.latest_prices.insert(symbol.to_string(), price);

        if let Some(pos) = self.positions.get_mut(symbol) {
            pos.current_value = pos.amount * price;
            let cost_basis = pos.amount * pos.average_entry_price;
            pos.unrealized_pnl = pos.current_value - cost_basis;
        }
    }

    pub fn net_worth(&self) -> f64 {
        let mut worth = self.usdt_balance;

        for (sym, pos) in &self.positions {
            let px = match self.latest_prices.get(sym) {
                Some(p) => *p,
                None => 0.0,
            };
            worth += pos.amount * px;
        }

        worth
    }

    pub fn log_status(&self) {
        let symbols: Vec<&str> = self.positions.values().map(|p| p.symbol.as_str()).collect();
        info!(
            usdt_balance = self.usdt_balance,
            positions = self.positions.len(),
            symbols = ?symbols,
            net_worth = self.net_worth(),
            "wallet status"
        );
    }

    pub fn execute_buy(&mut self, symbol: &str, price: f64, usdt_amount: f64) {
        if self.usdt_balance < 10.0 {
            info!(
                symbol = symbol,
                balance = self.usdt_balance,
                "BUY skipped: insufficient funds (<10 USDT)"
            );
            return;
        }

        let spend = usdt_amount.min(self.usdt_balance);
        if spend <= 0.0 {
            return;
        }

        let mut rng = rand::thread_rng();
        let slippage_pct: f64 = rng.gen_range(0.0..=0.0005);
        let exec_price = price * (1.0 + slippage_pct);

        let trading_fee = spend * 0.001;
        let net_spend = spend - trading_fee;
        if net_spend <= 0.0 {
            info!(symbol = symbol, "BUY skipped: spend too small after fee");
            return;
        }

        let qty = net_spend / exec_price;

        // Update balance first (spend includes the fee).
        self.usdt_balance -= spend;

        // Update or create position
        match self.positions.get_mut(symbol) {
            Some(pos) => {
                let old_amount = pos.amount;
                let old_avg = pos.average_entry_price;

                let new_amount = old_amount + qty;
                let new_avg = if new_amount > 0.0 {
                    (old_amount * old_avg + qty * exec_price) / new_amount
                } else {
                    exec_price
                };

                pos.amount = new_amount;
                pos.average_entry_price = new_avg;

                // Mark-to-market using current price (not exec price) for realism
                self.update_mark(symbol, price);
            }
            None => {
                let pos = Position {
                    symbol: symbol.to_string(),
                    amount: qty,
                    average_entry_price: exec_price,
                    current_value: 0.0,
                    unrealized_pnl: 0.0,
                };
                // Insert then mark
                self.positions.insert(symbol.to_string(), pos);
                self.update_mark(symbol, price);
            }
        }

        info!(
            "BUY executed: [{}] @ [{:.6}] | Fee: [{:.4}] | Balance: [{:.4}]",
            symbol, exec_price, trading_fee, self.usdt_balance
        );
        info!(
            symbol = symbol,
            exec_price = exec_price,
            qty = qty,
            slippage_pct = slippage_pct,
            fee = trading_fee,
            "buy details"
        );
    }

    pub fn execute_sell(&mut self, symbol: &str, price: f64) {
        let pos = match self.positions.remove(symbol) {
            Some(p) => p,
            None => return,
        };

        let mut rng = rand::thread_rng();
        let slippage_pct: f64 = rng.gen_range(0.0..=0.0005);
        let exec_price = price * (1.0 - slippage_pct);

        let gross = pos.amount * exec_price;
        let fee = gross * 0.001;
        let net = gross - fee;

        let cost_basis = pos.amount * pos.average_entry_price;
        let pnl = net - cost_basis;

        self.usdt_balance += net;

        // Update mark cache
        self.latest_prices.insert(symbol.to_string(), price);

        info!(
            "SELL executed: [{}] | PnL: [{:.4}] | New Balance: [{:.4}]",
            symbol, pnl, self.usdt_balance
        );
        info!(
            symbol = symbol,
            exec_price = exec_price,
            amount = pos.amount,
            slippage_pct = slippage_pct,
            fee = fee,
            gross = gross,
            net = net,
            cost_basis = cost_basis,
            pnl = pnl,
            "sell details"
        );
    }
}

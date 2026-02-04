//! Wallet and Position for perpetual futures simulation.
//! Positions use mark price for unrealized PnL; funding is applied at funding timestamps.

use rand::Rng;
use std::collections::HashMap;
use tracing::{debug, info};

use crate::model::{norm_symbol, MarketContextEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSide {
    Long,
    Short,
}

/// Perpetual futures position: side, size, entry, leverage, margin, liquidation estimate.
/// leverage and liquidation_price are stored for risk/display; may be read by UI or risk logic.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Position {
    pub symbol: String,
    pub side: PositionSide,
    pub amount: f64,
    pub average_entry_price: f64,
    pub leverage: u8,
    /// margin_used = (entry_price * amount) / leverage
    pub margin_used: f64,
    /// size_notional = entry_price * amount
    pub size_notional: f64,
    /// Estimated liquidation price (approximation; see doc in wallet).
    pub liquidation_price: f64,
    /// current_value = mark_price * amount (always positive notional)
    pub current_value: f64,
    /// Unrealized PnL computed with mark price: Long => (mark - entry)*amount, Short => (entry - mark)*amount
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone)]
pub struct FundingState {
    pub funding_rate: f64,
    pub next_funding_time: i64,
    pub last_applied_funding_time: i64,
}

#[derive(Debug)]
pub struct Wallet {
    pub usdt_balance: f64,
    pub positions: HashMap<String, Position>,
    pub mark_prices: HashMap<String, f64>,
    pub funding: HashMap<String, FundingState>,
    pub open_interest: HashMap<String, f64>,
}

impl Wallet {
    pub fn new() -> Self {
        Self {
            usdt_balance: 1000.0,
            positions: HashMap::new(),
            mark_prices: HashMap::new(),
            funding: HashMap::new(),
            open_interest: HashMap::new(),
        }
    }

    pub fn has_position(&self, symbol: &str) -> bool {
        self.positions.contains_key(&norm_symbol(symbol))
    }

    #[allow(dead_code)]
    pub fn position_entry_price(&self, symbol: &str) -> Option<f64> {
        self.positions
            .get(&norm_symbol(symbol))
            .map(|p| p.average_entry_price)
    }

    /// Update wallet state from a market.context event (mark price, funding, OI).
    pub fn update_context(&mut self, evt: &MarketContextEvent) {
        let symbol = norm_symbol(&evt.symbol);
        if let Some(mp) = evt.mark_price {
            if mp.is_finite() {
                self.mark_prices.insert(symbol.clone(), mp);
            }
        }
        if let Some(oi) = evt.open_interest {
            if oi.is_finite() {
                self.open_interest.insert(symbol.clone(), oi);
            }
        }
        if let (Some(rate), Some(next_ts)) = (evt.funding_rate, evt.next_funding_time) {
            if rate.is_finite() {
                let entry = self.funding.entry(symbol).or_insert(FundingState {
                    funding_rate: 0.0,
                    next_funding_time: 0,
                    last_applied_funding_time: 0,
                });
                entry.funding_rate = rate;
                entry.next_funding_time = next_ts;
            }
        }
    }

    /// Recompute unrealized PnL and current_value for all positions using mark_prices.
    pub fn update_pnl(&mut self) {
        for (sym, pos) in self.positions.iter_mut() {
            let mark = match self.mark_prices.get(sym) {
                Some(&m) if m.is_finite() && m > 0.0 => m,
                _ => {
                    debug!(symbol = %sym, "mark price missing or invalid; skipping PnL update");
                    continue;
                }
            };
            pos.current_value = pos.amount * mark;
            pos.unrealized_pnl = match pos.side {
                PositionSide::Long => (mark - pos.average_entry_price) * pos.amount,
                PositionSide::Short => (pos.average_entry_price - mark) * pos.amount,
            };
        }
    }

    /// Apply funding once per funding timestamp per symbol (deduct size_notional * funding_rate from usdt_balance).
    pub fn apply_funding_if_due(&mut self, now_ms: i64) {
        let syms_due: Vec<_> = self
            .positions
            .iter()
            .filter_map(|(sym, pos)| {
                let fund = self.funding.get(sym)?;
                if now_ms >= fund.next_funding_time
                    && fund.last_applied_funding_time < fund.next_funding_time
                {
                    Some((
                        sym.clone(),
                        pos.size_notional,
                        fund.funding_rate,
                        fund.next_funding_time,
                    ))
                } else {
                    None
                }
            })
            .collect();
        for (sym, size_notional, funding_rate, next_ts) in syms_due {
            let fee = size_notional * funding_rate;
            self.usdt_balance -= fee;
            if let Some(f) = self.funding.get_mut(&sym) {
                f.last_applied_funding_time = next_ts;
            }
            info!(
                symbol = %sym,
                rate = funding_rate,
                fee = fee,
                new_balance = self.usdt_balance,
                "FUNDING: applied"
            );
        }
    }

    /// Estimated liquidation price (simplified; long: entry*(1 - 1/lev), short: entry*(1 + 1/lev)).
    fn liquidation_price_estimate(side: PositionSide, entry: f64, leverage: u8) -> f64 {
        let lev_f = leverage as f64;
        let liq = match side {
            PositionSide::Long => entry * (1.0 - 1.0 / lev_f),
            PositionSide::Short => entry * (1.0 + 1.0 / lev_f),
        };
        if side == PositionSide::Long && liq < 0.0 {
            0.0
        } else {
            liq
        }
    }

    pub fn net_worth(&self) -> f64 {
        let mut worth = self.usdt_balance;
        for (sym, pos) in &self.positions {
            let mark = self.mark_prices.get(sym).copied().unwrap_or(0.0);
            let pnl = match pos.side {
                PositionSide::Long => (mark - pos.average_entry_price) * pos.amount,
                PositionSide::Short => (pos.average_entry_price - mark) * pos.amount,
            };
            worth += pos.margin_used + pnl;
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

    /// Get reference price for execution (mark if available, else trade price).
    fn exec_reference_price(&self, symbol: &str, trade_price: f64) -> f64 {
        self.mark_prices
            .get(&norm_symbol(symbol))
            .copied()
            .filter(|p| p.is_finite() && *p > 0.0)
            .unwrap_or(trade_price)
    }

    /// Open long: deduct margin, create position. leverage default 1.
    pub fn execute_open_long(
        &mut self,
        symbol: &str,
        trade_price: f64,
        usdt_margin: f64,
        leverage: u8,
    ) {
        self.execute_open_long_with_slippage(symbol, trade_price, usdt_margin, leverage, None);
    }

    pub fn execute_open_long_with_slippage(
        &mut self,
        symbol: &str,
        trade_price: f64,
        usdt_margin: f64,
        leverage: u8,
        slippage_override: Option<f64>,
    ) {
        let symbol = norm_symbol(symbol);
        if self.usdt_balance < 10.0 {
            info!(symbol = %symbol, balance = self.usdt_balance, "OPEN LONG skipped: insufficient balance");
            return;
        }
        if self.positions.contains_key(&symbol) {
            debug!(symbol = %symbol, "OPEN LONG skipped: already have position");
            return;
        }
        let lev = leverage.max(1);
        let ref_price = self.exec_reference_price(&symbol, trade_price);
        let slippage_pct = slippage_override.unwrap_or_else(|| {
            let mut rng = rand::thread_rng();
            rng.gen_range(0.0..=0.0005)
        });
        let exec_price = ref_price * (1.0 + slippage_pct);
        let notional = usdt_margin * (lev as f64);
        let amount = notional / exec_price;
        let margin_used = usdt_margin;
        let fee = notional * 0.0004f64;
        if self.usdt_balance < margin_used + fee {
            info!(symbol = %symbol, "OPEN LONG skipped: margin + fee exceeds balance");
            return;
        }
        self.usdt_balance -= margin_used + fee;
        let liq = Self::liquidation_price_estimate(PositionSide::Long, exec_price, lev);
        let pos = Position {
            symbol: symbol.clone(),
            side: PositionSide::Long,
            amount,
            average_entry_price: exec_price,
            leverage: lev,
            margin_used,
            size_notional: notional,
            liquidation_price: liq,
            current_value: amount * exec_price,
            unrealized_pnl: 0.0,
        };
        self.positions.insert(symbol.clone(), pos);
        if let Some(mark) = self.mark_prices.get(&symbol).copied() {
            if mark.is_finite() {
                self.update_pnl();
            }
        }
        info!(
            symbol = %symbol,
            qty = amount,
            entry = exec_price,
            lev = lev,
            margin_used = margin_used,
            liq = liq,
            balance = self.usdt_balance,
            "OPEN LONG executed"
        );
    }

    /// Open short: deduct margin, create position.
    pub fn execute_open_short(
        &mut self,
        symbol: &str,
        trade_price: f64,
        usdt_margin: f64,
        leverage: u8,
    ) {
        self.execute_open_short_with_slippage(symbol, trade_price, usdt_margin, leverage, None);
    }

    pub fn execute_open_short_with_slippage(
        &mut self,
        symbol: &str,
        trade_price: f64,
        usdt_margin: f64,
        leverage: u8,
        slippage_override: Option<f64>,
    ) {
        let symbol = norm_symbol(symbol);
        if self.usdt_balance < 10.0 {
            info!(symbol = %symbol, balance = self.usdt_balance, "OPEN SHORT skipped: insufficient balance");
            return;
        }
        if self.positions.contains_key(&symbol) {
            debug!(symbol = %symbol, "OPEN SHORT skipped: already have position");
            return;
        }
        let lev = leverage.max(1);
        let ref_price = self.exec_reference_price(&symbol, trade_price);
        let slippage_pct = slippage_override.unwrap_or_else(|| {
            let mut rng = rand::thread_rng();
            rng.gen_range(0.0..=0.0005)
        });
        let exec_price = ref_price * (1.0 - slippage_pct);
        if exec_price <= 0.0 {
            return;
        }
        let notional = usdt_margin * (lev as f64);
        let amount = notional / exec_price;
        let margin_used = usdt_margin;
        let fee = notional * 0.0004f64;
        if self.usdt_balance < margin_used + fee {
            info!(symbol = %symbol, "OPEN SHORT skipped: margin + fee exceeds balance");
            return;
        }
        self.usdt_balance -= margin_used + fee;
        let liq = Self::liquidation_price_estimate(PositionSide::Short, exec_price, lev);
        let pos = Position {
            symbol: symbol.clone(),
            side: PositionSide::Short,
            amount,
            average_entry_price: exec_price,
            leverage: lev,
            margin_used,
            size_notional: notional,
            liquidation_price: liq,
            current_value: amount * exec_price,
            unrealized_pnl: 0.0,
        };
        self.positions.insert(symbol.clone(), pos);
        self.update_pnl();
        info!(
            symbol = %symbol,
            qty = amount,
            entry = exec_price,
            lev = lev,
            margin_used = margin_used,
            liq = liq,
            balance = self.usdt_balance,
            "OPEN SHORT executed"
        );
    }

    /// Close position: release margin + realized PnL - fee.
    pub fn execute_close(&mut self, symbol: &str, trade_price: f64) {
        self.execute_close_with_slippage(symbol, trade_price, None);
    }

    pub fn execute_close_with_slippage(
        &mut self,
        symbol: &str,
        trade_price: f64,
        slippage_override: Option<f64>,
    ) {
        let symbol = norm_symbol(symbol);
        let pos = match self.positions.remove(&symbol) {
            Some(p) => p,
            None => return,
        };
        let ref_price = self.exec_reference_price(&symbol, trade_price);
        let slippage_pct = slippage_override.unwrap_or_else(|| {
            let mut rng = rand::thread_rng();
            rng.gen_range(0.0..=0.0005)
        });
        let exec_price = match pos.side {
            PositionSide::Long => ref_price * (1.0 - slippage_pct),
            PositionSide::Short => ref_price * (1.0 + slippage_pct),
        };
        let close_notional = pos.amount * exec_price;
        let fee = close_notional * 0.0004f64;
        let realized_pnl = match pos.side {
            PositionSide::Long => (exec_price - pos.average_entry_price) * pos.amount,
            PositionSide::Short => (pos.average_entry_price - exec_price) * pos.amount,
        };
        self.usdt_balance += pos.margin_used + realized_pnl - fee;
        info!(
            symbol = %symbol,
            exit = exec_price,
            realized_pnl = realized_pnl,
            balance = self.usdt_balance,
            "CLOSE executed"
        );
    }

    // ---- Legacy spot-style entry points: map to futures open/close (used by tests and optional callers) ----
    /// Legacy: execute_buy => open long with default leverage 1 and given USDT as margin.
    #[allow(dead_code)]
    pub fn execute_buy(&mut self, symbol: &str, price: f64, usdt_amount: f64) {
        self.execute_open_long(symbol, price, usdt_amount, 1);
    }

    #[allow(dead_code)]
    pub fn execute_buy_with_slippage(
        &mut self,
        symbol: &str,
        price: f64,
        usdt_amount: f64,
        slippage_override: Option<f64>,
    ) {
        self.execute_open_long_with_slippage(symbol, price, usdt_amount, 1, slippage_override);
    }

    #[allow(dead_code)]
    pub fn execute_sell(&mut self, symbol: &str, price: f64) {
        self.execute_close(symbol, price);
    }

    #[allow(dead_code)]
    pub fn execute_sell_with_slippage(
        &mut self,
        symbol: &str,
        price: f64,
        slippage_override: Option<f64>,
    ) {
        self.execute_close_with_slippage(symbol, price, slippage_override);
    }
}

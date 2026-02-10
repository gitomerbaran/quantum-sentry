//! Paper broker: deterministic execution of OrderCommand using BBO prices.
//! Spot-like trading (1x, no leverage in v1) with configurable fees and slippage.

use common::{BboTick, OrderCommand, OrderSide, OrderType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::info;

/// Paper broker configuration.
#[derive(Debug, Clone)]
pub struct PaperBrokerConfig {
    pub initial_cash_usdt: f64,
    pub fee_bps: f64,              // e.g., 7.5 = 0.075%
    pub slippage_bps: f64,         // e.g., 2.0 = 0.02%
    pub max_position_notional_usdt: f64,
    pub allow_short: bool,
}

impl Default for PaperBrokerConfig {
    fn default() -> Self {
        Self {
            initial_cash_usdt: 100.0,
            fee_bps: 7.5,
            slippage_bps: 2.0,
            max_position_notional_usdt: 100.0,
            allow_short: true,
        }
    }
}

impl PaperBrokerConfig {
    pub fn from_env() -> Self {
        let initial_cash = std::env::var("INITIAL_CASH_USDT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100.0);
        let fee_bps = std::env::var("FEE_BPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(7.5);
        let slippage_bps = std::env::var("SLIPPAGE_BPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2.0);
        let max_notional = std::env::var("MAX_POSITION_NOTIONAL_USDT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100.0);
        let allow_short = std::env::var("ALLOW_SHORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(true);

        Self {
            initial_cash_usdt: initial_cash,
            fee_bps,
            slippage_bps,
            max_position_notional_usdt: max_notional,
            allow_short,
        }
    }
}

/// Position side for paper trading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PositionSide {
    Flat,
    Long,
    Short,
}

/// Per-symbol portfolio state.
#[derive(Debug, Clone)]
pub struct SymbolPortfolio {
    pub symbol: String,
    pub position_side: PositionSide,
    pub position_qty: f64,
    pub entry_price_avg: f64,
    pub realized_pnl_usdt: f64,
    pub fees_paid_usdt: f64,
    pub trades_total: u32,
    pub wins_total: u32,
    pub losses_total: u32,
}

impl SymbolPortfolio {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            position_side: PositionSide::Flat,
            position_qty: 0.0,
            entry_price_avg: 0.0,
            realized_pnl_usdt: 0.0,
            fees_paid_usdt: 0.0,
            trades_total: 0,
            wins_total: 0,
            losses_total: 0,
        }
    }

    pub fn win_rate(&self) -> f64 {
        if self.trades_total == 0 {
            0.0
        } else {
            self.wins_total as f64 / self.trades_total as f64
        }
    }
}

/// Fill event (execution result).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub ts_exchange: i64,
    pub symbol: String,
    pub order_id: String,
    pub action: String, // "OPEN_LONG", "CLOSE_LONG", "OPEN_SHORT", "CLOSE_SHORT"
    pub qty: f64,
    pub fill_price: f64,
    pub notional: f64,
    pub fee: f64,
    pub slippage_bps: f32,
    pub reason_code: String, // empty if filled, else rejection reason
}

/// Account state snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub ts_exchange: i64,
    pub symbol: String,
    pub cash_usdt: f64,
    pub position_side: String, // "FLAT", "LONG", "SHORT"
    pub position_qty: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub equity_usdt: f64,
    pub fees_paid: f64,
    pub trades_total: u32,
    pub wins_total: u32,
    pub losses_total: u32,
}

/// Paper broker: executes OrderCommands using BBO prices.
pub struct PaperBroker {
    config: PaperBrokerConfig,
    cash_usdt: f64,
    portfolios: HashMap<String, SymbolPortfolio>,
    // Latest BBO per symbol (for execution price)
    latest_bbo: HashMap<String, (BboTick, i64)>, // (bbo, ts_exchange)
}

impl PaperBroker {
    pub fn new(config: PaperBrokerConfig) -> Self {
        Self {
            cash_usdt: config.initial_cash_usdt,
            portfolios: HashMap::new(),
            latest_bbo: HashMap::new(),
            config,
        }
    }

    /// Update latest BBO (for execution price lookup).
    pub fn update_bbo(&mut self, bbo: &BboTick) {
        let symbol = bbo.symbol.to_uppercase();
        let entry = self.latest_bbo.entry(symbol.clone()).or_insert_with(|| {
            (
                BboTick {
                    ts_exchange: 0,
                    ts_ingest: 0,
                    update_id: 0,
                    symbol: symbol.clone(),
                    bid_price: 0.0,
                    bid_qty: 0.0,
                    ask_price: 0.0,
                    ask_qty: 0.0,
                },
                0,
            )
        });
        // Update if this BBO is at or after current timestamp
        if bbo.ts_exchange >= entry.1 {
            entry.0 = bbo.clone();
            entry.1 = bbo.ts_exchange;
        }
    }

    /// Get latest BBO for symbol at or before ts_exchange.
    ///
    /// NOTE: Return an owned `BboTick` to avoid holding an immutable borrow of `self`
    /// across calls that need `&mut self` (prevents E0502).
    fn get_bbo_at(&self, symbol: &str, ts_exchange: i64) -> Option<BboTick> {
        let symbol = symbol.to_uppercase();
        self.latest_bbo.get(&symbol).and_then(|(bbo, ts)| {
            if *ts <= ts_exchange {
                Some(bbo.clone())
            } else {
                None
            }
        })
    }

    /// Execute an OrderCommand. Returns Fill (may be rejected with reason_code).
    pub fn execute_order(&mut self, cmd: &OrderCommand, ts_exchange: i64) -> Fill {
        let symbol = cmd.symbol.to_uppercase();

        // Get BBO for execution price
        let bbo = match self.get_bbo_at(&symbol, ts_exchange) {
            Some(b) => b,
            None => {
                return Fill {
                    ts_exchange,
                    symbol: symbol.clone(),
                    order_id: cmd.client_order_id.clone(),
                    action: "REJECTED".to_string(),
                    qty: 0.0,
                    fill_price: 0.0,
                    notional: 0.0,
                    fee: 0.0,
                    slippage_bps: 0.0,
                    reason_code: "no_market_price".to_string(),
                };
            }
        };

        // Only market orders supported in v1
        if cmd.order_type != OrderType::Market {
            return Fill {
                ts_exchange,
                symbol: symbol.clone(),
                order_id: cmd.client_order_id.clone(),
                action: "REJECTED".to_string(),
                qty: 0.0,
                fill_price: 0.0,
                notional: 0.0,
                fee: 0.0,
                slippage_bps: 0.0,
                reason_code: "unsupported_order_type".to_string(),
            };
        }

        // Determine action from OrderSide and current position (before mutable borrow)
        let position_side = self
            .portfolios
            .get(&symbol)
            .map(|p| p.position_side)
            .unwrap_or(PositionSide::Flat);
        // Clone config to avoid borrow conflicts
        let allow_short = self.config.allow_short;
        let action = Self::determine_action_static(cmd, position_side, allow_short);

        let symbol_clone = symbol.clone();

        // Execute based on action - use entry() in each branch to avoid borrow conflicts
        match action.as_str() {
            "OPEN_LONG" => {
                self.execute_open_long(cmd, &bbo, &symbol_clone, ts_exchange)
            }
            "CLOSE_LONG" => {
                self.execute_close_long(cmd, &bbo, &symbol_clone, ts_exchange)
            }
            "OPEN_SHORT" => {
                self.execute_open_short(cmd, &bbo, &symbol_clone, ts_exchange)
            }
            "CLOSE_SHORT" => {
                self.execute_close_short(cmd, &bbo, &symbol_clone, ts_exchange)
            }
            _ => Fill {
                ts_exchange,
                symbol: symbol_clone,
                order_id: cmd.client_order_id.clone(),
                action: "REJECTED".to_string(),
                qty: 0.0,
                fill_price: 0.0,
                notional: 0.0,
                fee: 0.0,
                slippage_bps: 0.0,
                reason_code: action,
            },
        }
    }

    fn determine_action(&self, cmd: &OrderCommand, position_side: PositionSide) -> String {
        Self::determine_action_static(cmd, position_side, self.config.allow_short)
    }

    fn determine_action_static(cmd: &OrderCommand, position_side: PositionSide, allow_short: bool) -> String {
        match (cmd.side, position_side) {
            (OrderSide::Buy, PositionSide::Flat) => "OPEN_LONG".to_string(),
            (OrderSide::Sell, PositionSide::Flat) => {
                if allow_short {
                    "OPEN_SHORT".to_string()
                } else {
                    "short_disabled".to_string()
                }
            }
            (OrderSide::Sell, PositionSide::Long) => "CLOSE_LONG".to_string(),
            (OrderSide::Buy, PositionSide::Short) => "CLOSE_SHORT".to_string(),
            (OrderSide::Buy, PositionSide::Long) => "position_conflict".to_string(),
            (OrderSide::Sell, PositionSide::Short) => "position_conflict".to_string(),
        }
    }

    fn execute_open_long(
        &mut self,
        cmd: &OrderCommand,
        bbo: &BboTick,
        symbol: &str,
        ts_exchange: i64,
    ) -> Fill {
        let portfolio = self
            .portfolios
            .entry(symbol.to_string())
            .or_insert_with(|| SymbolPortfolio::new(symbol.to_string()));

        // Calculate execution price (ask + slippage)
        let slippage_pct = self.config.slippage_bps / 10_000.0;
        let fill_price = bbo.ask_price * (1.0 + slippage_pct);
        let qty = cmd.qty;
        let notional = fill_price * qty;

        // Check notional cap
        if notional > self.config.max_position_notional_usdt {
            return Fill {
                ts_exchange,
                symbol: cmd.symbol.clone(),
                order_id: cmd.client_order_id.clone(),
                action: "REJECTED".to_string(),
                qty: 0.0,
                fill_price: 0.0,
                notional: 0.0,
                fee: 0.0,
                slippage_bps: 0.0,
                reason_code: "notional_cap".to_string(),
            };
        }

        // Calculate fee
        let fee = notional * (self.config.fee_bps / 10_000.0);
        let total_cost = notional + fee;

        // Check cash
        if self.cash_usdt < total_cost {
            return Fill {
                ts_exchange,
                symbol: cmd.symbol.clone(),
                order_id: cmd.client_order_id.clone(),
                action: "REJECTED".to_string(),
                qty: 0.0,
                fill_price: 0.0,
                notional: 0.0,
                fee: 0.0,
                slippage_bps: 0.0,
                reason_code: "insufficient_cash".to_string(),
            };
        }

        // Execute
        self.cash_usdt -= total_cost;
        portfolio.position_side = PositionSide::Long;
        portfolio.position_qty = qty;
        portfolio.entry_price_avg = fill_price;
        portfolio.fees_paid_usdt += fee;

        info!(
            symbol = %cmd.symbol,
            order_id = %cmd.client_order_id,
            qty = qty,
            fill_price = fill_price,
            fee = fee,
            cash = self.cash_usdt,
            "OPEN_LONG executed"
        );

        Fill {
            ts_exchange,
            symbol: cmd.symbol.clone(),
            order_id: cmd.client_order_id.clone(),
            action: "OPEN_LONG".to_string(),
            qty,
            fill_price,
            notional,
            fee,
            slippage_bps: self.config.slippage_bps as f32,
            reason_code: String::new(),
        }
    }

    fn execute_close_long(
        &mut self,
        cmd: &OrderCommand,
        bbo: &BboTick,
        symbol: &str,
        ts_exchange: i64,
    ) -> Fill {
        let portfolio = self
            .portfolios
            .entry(symbol.to_string())
            .or_insert_with(|| SymbolPortfolio::new(symbol.to_string()));

        if portfolio.position_side != PositionSide::Long {
            return Fill {
                ts_exchange,
                symbol: cmd.symbol.clone(),
                order_id: cmd.client_order_id.clone(),
                action: "REJECTED".to_string(),
                qty: 0.0,
                fill_price: 0.0,
                notional: 0.0,
                fee: 0.0,
                slippage_bps: 0.0,
                reason_code: "position_conflict".to_string(),
            };
        }

        let slippage_pct = self.config.slippage_bps / 10_000.0;
        let fill_price = bbo.bid_price * (1.0 - slippage_pct);
        let qty = portfolio.position_qty.min(cmd.qty);
        let notional = fill_price * qty;
        let fee = notional * (self.config.fee_bps / 10_000.0);

        // Calculate realized PnL
        let realized_pnl = (fill_price - portfolio.entry_price_avg) * qty - fee;

        // Update portfolio
        self.cash_usdt += notional - fee;
        portfolio.realized_pnl_usdt += realized_pnl;
        portfolio.fees_paid_usdt += fee;
        portfolio.trades_total += 1;
        if realized_pnl > 0.0 {
            portfolio.wins_total += 1;
        } else if realized_pnl < 0.0 {
            portfolio.losses_total += 1;
        }

        // Close position
        portfolio.position_side = PositionSide::Flat;
        portfolio.position_qty = 0.0;
        portfolio.entry_price_avg = 0.0;

        info!(
            symbol = %cmd.symbol,
            order_id = %cmd.client_order_id,
            qty = qty,
            fill_price = fill_price,
            realized_pnl = realized_pnl,
            cash = self.cash_usdt,
            "CLOSE_LONG executed"
        );

        Fill {
            ts_exchange,
            symbol: cmd.symbol.clone(),
            order_id: cmd.client_order_id.clone(),
            action: "CLOSE_LONG".to_string(),
            qty,
            fill_price,
            notional,
            fee,
            slippage_bps: self.config.slippage_bps as f32,
            reason_code: String::new(),
        }
    }

    fn execute_open_short(
        &mut self,
        cmd: &OrderCommand,
        bbo: &BboTick,
        symbol: &str,
        ts_exchange: i64,
    ) -> Fill {
        if !self.config.allow_short {
            return Fill {
                ts_exchange,
                symbol: cmd.symbol.clone(),
                order_id: cmd.client_order_id.clone(),
                action: "REJECTED".to_string(),
                qty: 0.0,
                fill_price: 0.0,
                notional: 0.0,
                fee: 0.0,
                slippage_bps: 0.0,
                reason_code: "short_disabled".to_string(),
            };
        }

        let portfolio = self
            .portfolios
            .entry(symbol.to_string())
            .or_insert_with(|| SymbolPortfolio::new(symbol.to_string()));

        let slippage_pct = self.config.slippage_bps / 10_000.0;
        let fill_price = bbo.bid_price * (1.0 - slippage_pct);
        let qty = cmd.qty;
        let notional = fill_price * qty;

        if notional > self.config.max_position_notional_usdt {
            return Fill {
                ts_exchange,
                symbol: cmd.symbol.clone(),
                order_id: cmd.client_order_id.clone(),
                action: "REJECTED".to_string(),
                qty: 0.0,
                fill_price: 0.0,
                notional: 0.0,
                fee: 0.0,
                slippage_bps: 0.0,
                reason_code: "notional_cap".to_string(),
            };
        }

        let fee = notional * (self.config.fee_bps / 10_000.0);
        let total_cost = notional + fee;

        if self.cash_usdt < total_cost {
            return Fill {
                ts_exchange,
                symbol: cmd.symbol.clone(),
                order_id: cmd.client_order_id.clone(),
                action: "REJECTED".to_string(),
                qty: 0.0,
                fill_price: 0.0,
                notional: 0.0,
                fee: 0.0,
                slippage_bps: 0.0,
                reason_code: "insufficient_cash".to_string(),
            };
        }

        self.cash_usdt -= total_cost;
        portfolio.position_side = PositionSide::Short;
        portfolio.position_qty = qty;
        portfolio.entry_price_avg = fill_price;
        portfolio.fees_paid_usdt += fee;

        info!(
            symbol = %cmd.symbol,
            order_id = %cmd.client_order_id,
            qty = qty,
            fill_price = fill_price,
            fee = fee,
            cash = self.cash_usdt,
            "OPEN_SHORT executed"
        );

        Fill {
            ts_exchange,
            symbol: cmd.symbol.clone(),
            order_id: cmd.client_order_id.clone(),
            action: "OPEN_SHORT".to_string(),
            qty,
            fill_price,
            notional,
            fee,
            slippage_bps: self.config.slippage_bps as f32,
            reason_code: String::new(),
        }
    }

    fn execute_close_short(
        &mut self,
        cmd: &OrderCommand,
        bbo: &BboTick,
        symbol: &str,
        ts_exchange: i64,
    ) -> Fill {
        let portfolio = self
            .portfolios
            .entry(symbol.to_string())
            .or_insert_with(|| SymbolPortfolio::new(symbol.to_string()));

        if portfolio.position_side != PositionSide::Short {
            return Fill {
                ts_exchange,
                symbol: cmd.symbol.clone(),
                order_id: cmd.client_order_id.clone(),
                action: "REJECTED".to_string(),
                qty: 0.0,
                fill_price: 0.0,
                notional: 0.0,
                fee: 0.0,
                slippage_bps: 0.0,
                reason_code: "position_conflict".to_string(),
            };
        }

        let slippage_pct = self.config.slippage_bps / 10_000.0;
        let fill_price = bbo.ask_price * (1.0 + slippage_pct);
        let qty = portfolio.position_qty.min(cmd.qty);
        let notional = fill_price * qty;
        let fee = notional * (self.config.fee_bps / 10_000.0);

        // For short: realized PnL = (entry - fill) * qty - fee
        let realized_pnl = (portfolio.entry_price_avg - fill_price) * qty - fee;

        self.cash_usdt += notional - fee;
        portfolio.realized_pnl_usdt += realized_pnl;
        portfolio.fees_paid_usdt += fee;
        portfolio.trades_total += 1;
        if realized_pnl > 0.0 {
            portfolio.wins_total += 1;
        } else if realized_pnl < 0.0 {
            portfolio.losses_total += 1;
        }

        portfolio.position_side = PositionSide::Flat;
        portfolio.position_qty = 0.0;
        portfolio.entry_price_avg = 0.0;

        info!(
            symbol = %cmd.symbol,
            order_id = %cmd.client_order_id,
            qty = qty,
            fill_price = fill_price,
            realized_pnl = realized_pnl,
            cash = self.cash_usdt,
            "CLOSE_SHORT executed"
        );

        Fill {
            ts_exchange,
            symbol: cmd.symbol.clone(),
            order_id: cmd.client_order_id.clone(),
            action: "CLOSE_SHORT".to_string(),
            qty,
            fill_price,
            notional,
            fee,
            slippage_bps: self.config.slippage_bps as f32,
            reason_code: String::new(),
        }
    }

    /// Generate account snapshot for a symbol.
    pub fn account_snapshot(&self, symbol: &str, ts_exchange: i64) -> Option<AccountSnapshot> {
        let symbol = symbol.to_uppercase();
        let portfolio = self.portfolios.get(&symbol)?;
        let bbo = self.get_bbo_at(&symbol, ts_exchange)?;
        let mark_price = (bbo.bid_price + bbo.ask_price) / 2.0;

        let unrealized_pnl = match portfolio.position_side {
            PositionSide::Long => (mark_price - portfolio.entry_price_avg) * portfolio.position_qty,
            PositionSide::Short => (portfolio.entry_price_avg - mark_price) * portfolio.position_qty,
            PositionSide::Flat => 0.0,
        };

        let equity = self.cash_usdt + unrealized_pnl;

        Some(AccountSnapshot {
            ts_exchange,
            symbol: symbol.clone(),
            cash_usdt: self.cash_usdt,
            position_side: format!("{:?}", portfolio.position_side),
            position_qty: portfolio.position_qty,
            entry_price: portfolio.entry_price_avg,
            mark_price,
            unrealized_pnl,
            realized_pnl: portfolio.realized_pnl_usdt,
            equity_usdt: equity,
            fees_paid: portfolio.fees_paid_usdt,
            trades_total: portfolio.trades_total,
            wins_total: portfolio.wins_total,
            losses_total: portfolio.losses_total,
        })
    }

    /// Get all symbols with positions.
    pub fn active_symbols(&self) -> Vec<String> {
        self.portfolios
            .iter()
            .filter(|(_, p)| p.position_side != PositionSide::Flat)
            .map(|(s, _)| s.clone())
            .collect()
    }
}


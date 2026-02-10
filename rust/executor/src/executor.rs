//! Executor: consume FeatureSnapshot, apply gates + stabilization, output Decision (+ optional OrderCommand).
//! No DB. Shadow mode: OrderCommand only when execution_enabled.

use common::{Action, Decision, FeatureSnapshot, OrderCommand, OrderSide, OrderType};
use std::collections::HashMap;

use crate::config::ExecutorConfig;
use crate::inference::InferenceEngine;

/// Per-symbol executor state for stabilization.
#[derive(Debug, Clone)]
struct PerSymbolState {
    last_action: Action,
    last_trade_ts_ms: u64,
}

impl Default for PerSymbolState {
    fn default() -> Self {
        Self {
            last_action: Action::NoTrade,
            last_trade_ts_ms: 0,
        }
    }
}

/// Executor: gates + inference + stabilization -> Decision (+ OrderCommand when execution_enabled).
#[derive(Debug)]
pub struct Executor<E: InferenceEngine> {
    config: ExecutorConfig,
    engine: E,
    state: HashMap<String, PerSymbolState>,
}

impl<E: InferenceEngine> Executor<E> {
    pub fn new(config: ExecutorConfig, engine: E) -> Self {
        Self {
            config,
            engine,
            state: HashMap::new(),
        }
    }

    fn state_for(&mut self, symbol: &str) -> &mut PerSymbolState {
        self.state.entry(symbol.to_string()).or_default()
    }

    /// Process one FeatureSnapshot; returns (Decision, Option<OrderCommand>).
    /// OrderCommand only when execution_enabled and action is LONG or SHORT.
    pub fn on_snapshot(&mut self, snap: &FeatureSnapshot) -> (Decision, Option<OrderCommand>) {
        let mut reasons = Vec::new();
        let model_version = self.engine.model_version().to_string();

        // D1) Gates (collect all deterministic reasons for NO_TRADE)
        if !snap.ready {
            reasons.push("not_ready".to_string());
        }
        if snap.data_latency_ms > self.config.latency_max_ms {
            reasons.push("latency_too_high".to_string());
        }
        if !snap.spread_pct.is_finite() || snap.spread_pct > self.config.spread_max_pct {
            reasons.push("spread_too_high".to_string());
        }
        if let Some(expected_len) = self.engine.expected_feature_len() {
            if snap.features.len() != expected_len {
                reasons.push("meta_mismatch".to_string());
            }
        }
        if !reasons.is_empty() {
            let dec = self.decision(snap, Action::NoTrade, 0.0, &reasons, model_version.as_str());
            return (dec, None);
        }

        let out = self.engine.predict(&snap.features);
        if out.inference_error {
            reasons.push("inference_error".to_string());
            let dec = self.decision(snap, Action::NoTrade, 0.0, &reasons, model_version.as_str());
            return (dec, None);
        }
        if !out.confidence.is_finite() || out.confidence < self.config.confidence_min {
            reasons.push("confidence_low".to_string());
            let dec = self.decision(
                snap,
                Action::NoTrade,
                out.confidence,
                &reasons,
                model_version.as_str(),
            );
            return (dec, None);
        }

        // D2) Base action from inference
        let candidate = if out.p_up > out.p_down {
            reasons.push("model_long".to_string());
            Action::Long
        } else {
            reasons.push("model_short".to_string());
            Action::Short
        };

        if candidate == Action::Short && !self.config.allow_short {
            reasons.push("short_disabled".to_string());
            let dec = self.decision(
                snap,
                Action::NoTrade,
                out.confidence,
                &reasons,
                model_version.as_str(),
            );
            return (dec, None);
        }

        // Trace even when stabilization later turns action into NO_TRADE.
        reasons.push("passed_gates".to_string());
        reasons.push("signal_ok".to_string());

        // D3) Stabilization (copy config to avoid holding &mut self across use)
        let cooldown_ms = self.config.cooldown_ms;
        let hysteresis_exit = self.config.hysteresis_exit;
        let hysteresis_enter = self.config.hysteresis_enter;
        let sym_state = self.state_for(&snap.symbol);
        // Deterministic time base: use ts_exchange (epoch ms), not wall clock.
        let now = snap.ts_exchange.max(0) as u64;

        if candidate != Action::NoTrade
            && sym_state.last_trade_ts_ms != 0
            && now.saturating_sub(sym_state.last_trade_ts_ms) < cooldown_ms
        {
            reasons.push("cooldown_active".to_string());
            let dec = self.decision(
                snap,
                Action::NoTrade,
                out.confidence,
                &reasons,
                model_version.as_str(),
            );
            return (dec, None);
        }

        if out.confidence < hysteresis_exit
            && (sym_state.last_action == Action::Long || sym_state.last_action == Action::Short)
        {
            reasons.push("hysteresis_hold".to_string());
            let dec = self.decision(
                snap,
                Action::NoTrade,
                out.confidence,
                &reasons,
                model_version.as_str(),
            );
            return (dec, None);
        }

        let action = match (sym_state.last_action, candidate) {
            (Action::Long, Action::Short) if out.p_down < hysteresis_enter => {
                reasons.push("hysteresis_hold".to_string());
                Action::NoTrade
            }
            (Action::Short, Action::Long) if out.p_up < hysteresis_enter => {
                reasons.push("hysteresis_hold".to_string());
                Action::NoTrade
            }
            _ => candidate,
        };

        if action != Action::NoTrade {
            sym_state.last_action = action;
            sym_state.last_trade_ts_ms = now;
        }

        let dec = self.decision(
            snap,
            action,
            out.confidence,
            &reasons,
            model_version.as_str(),
        );

        let order = if self.config.execution_enabled
            && (action == Action::Long || action == Action::Short)
        {
            Some(self.order_command(snap, action))
        } else {
            None
        };

        (dec, order)
    }

    fn decision(
        &self,
        snap: &FeatureSnapshot,
        action: Action,
        confidence: f32,
        reason_codes: &[String],
        model_version: &str,
    ) -> Decision {
        Decision {
            ts_exchange: snap.ts_exchange,
            symbol: snap.symbol.clone(),
            action,
            confidence,
            reason_codes: reason_codes.to_vec(),
            model_version: model_version.to_string(),
        }
    }

    fn order_command(&self, snap: &FeatureSnapshot, action: Action) -> OrderCommand {
        let client_order_id = format!("{}-{}", snap.ts_exchange, uuid::Uuid::new_v4());
        let (side, qty) = match action {
            Action::Long => (OrderSide::Buy, 0.0), // TODO: qty from config/risk
            Action::Short => (OrderSide::Sell, 0.0), // TODO
            Action::NoTrade => unreachable!(),
        };
        OrderCommand {
            client_order_id,
            symbol: snap.symbol.clone(),
            side,
            order_type: OrderType::Market,
            qty,
            price: None,
            reduce_only: None,
            leverage: None,
            sl: None,
            tp: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::{MockInferenceEngine, MockInferenceEngineFails};

    fn snapshot(
        ready: bool,
        data_latency_ms: u32,
        spread_pct: f32,
        features: Vec<f32>,
    ) -> FeatureSnapshot {
        FeatureSnapshot {
            ts_exchange: 1000,
            symbol: "BTCUSDT".to_string(),
            features,
            ready,
            data_latency_ms,
            spread_pct,
            feature_version: 1,
        }
    }

    #[test]
    fn gate_ready_false_no_trade() {
        let config = ExecutorConfig::from_env_or_default();
        let engine = MockInferenceEngine::long_bias();
        let mut exec = Executor::new(config, engine);
        let snap = snapshot(false, 50, 0.01, vec![0.1, 0.0, 0.01, 0.5]);
        let (dec, order) = exec.on_snapshot(&snap);
        assert_eq!(dec.action, Action::NoTrade);
        assert!(dec.reason_codes.contains(&"not_ready".to_string()));
        assert!(order.is_none());
    }

    #[test]
    fn gate_latency_too_high_no_trade() {
        let config = ExecutorConfig::from_env_or_default();
        let mut c = config;
        c.latency_max_ms = 100;
        let engine = MockInferenceEngine::long_bias();
        let mut exec = Executor::new(c, engine);
        let snap = snapshot(true, 150, 0.01, vec![0.1, 0.0, 0.01, 0.5]);
        let (dec, _) = exec.on_snapshot(&snap);
        assert_eq!(dec.action, Action::NoTrade);
        assert!(dec.reason_codes.contains(&"latency_too_high".to_string()));
    }

    #[test]
    fn gate_spread_too_high_no_trade() {
        let config = ExecutorConfig::from_env_or_default();
        let mut c = config;
        c.spread_max_pct = 0.10;
        let engine = MockInferenceEngine::long_bias();
        let mut exec = Executor::new(c, engine);
        let snap = snapshot(true, 50, 0.25, vec![0.1, 0.0, 0.01, 0.5]);
        let (dec, _) = exec.on_snapshot(&snap);
        assert_eq!(dec.action, Action::NoTrade);
        assert!(dec.reason_codes.contains(&"spread_too_high".to_string()));
    }

    #[test]
    fn gate_low_confidence_no_trade() {
        let config = ExecutorConfig::from_env_or_default();
        let mut c = config;
        c.confidence_min = 0.8;
        let engine = MockInferenceEngine::low_confidence();
        let mut exec = Executor::new(c, engine);
        let snap = snapshot(true, 50, 0.01, vec![0.1, 0.0, 0.01, 0.5]);
        let (dec, _) = exec.on_snapshot(&snap);
        assert_eq!(dec.action, Action::NoTrade);
        assert!(dec.reason_codes.contains(&"confidence_low".to_string()));
    }

    #[test]
    fn shadow_mode_no_order_command() {
        let config = ExecutorConfig::from_env_or_default();
        assert!(!config.execution_enabled);
        let engine = MockInferenceEngine::long_bias();
        let mut exec = Executor::new(config, engine);
        let snap = snapshot(true, 50, 0.01, vec![0.1, 0.0, 0.01, 0.5]);
        let (dec, order) = exec.on_snapshot(&snap);
        assert!(dec.action == Action::Long || dec.action == Action::NoTrade);
        assert!(order.is_none());
    }

    #[test]
    fn execution_enabled_emits_order_command() {
        let mut config = ExecutorConfig::from_env_or_default();
        config.execution_enabled = true;
        config.cooldown_ms = 0;
        config.confidence_min = 0.3;
        let engine = MockInferenceEngine::long_bias();
        let mut exec = Executor::new(config, engine);
        let snap = snapshot(true, 50, 0.01, vec![0.1, 0.0, 0.01, 0.5]);
        let (dec, order) = exec.on_snapshot(&snap);
        assert_eq!(dec.action, Action::Long);
        let cmd = order.expect("OrderCommand when execution_enabled");
        assert!(!cmd.client_order_id.is_empty());
        assert_eq!(cmd.symbol, "BTCUSDT");
        assert_eq!(cmd.side, OrderSide::Buy);
    }

    #[test]
    fn cooldown_prevents_repeated_trades() {
        let mut config = ExecutorConfig::from_env_or_default();
        config.execution_enabled = false;
        config.cooldown_ms = 999_999;
        config.confidence_min = 0.3;
        let engine = MockInferenceEngine::long_bias();
        let mut exec = Executor::new(config, engine);
        let snap = snapshot(true, 50, 0.01, vec![0.1, 0.0, 0.01, 0.5]);
        let (dec1, _) = exec.on_snapshot(&snap);
        assert_eq!(dec1.action, Action::Long);
        let snap2 = snapshot(true, 51, 0.01, vec![0.1, 0.0, 0.01, 0.5]);
        let (dec2, _) = exec.on_snapshot(&snap2);
        assert_eq!(dec2.action, Action::NoTrade);
        assert!(dec2.reason_codes.contains(&"cooldown_active".to_string()));
    }

    #[test]
    fn hysteresis_prevents_immediate_flip() {
        use crate::inference::MockInferenceEngineFlip;
        let mut config = ExecutorConfig::from_env_or_default();
        config.execution_enabled = false;
        config.cooldown_ms = 0;
        config.confidence_min = 0.3;
        config.hysteresis_enter = 0.80;
        let engine = MockInferenceEngineFlip::new();
        let mut exec = Executor::new(config, engine);
        let snap1 = snapshot(true, 50, 0.01, vec![0.1, 0.0, 0.01, 0.5]);
        let (dec1, _) = exec.on_snapshot(&snap1);
        assert_eq!(dec1.action, Action::Long);
        let snap2 = snapshot(true, 51, 0.01, vec![0.1, 0.0, 0.01, 0.5]);
        let (dec2, _) = exec.on_snapshot(&snap2);
        assert_eq!(dec2.action, Action::NoTrade);
        assert!(dec2.reason_codes.contains(&"hysteresis_hold".to_string()));
    }

    #[test]
    fn meta_mismatch_when_feature_len_differs_from_expected() {
        let config = ExecutorConfig::from_env_or_default();
        let engine = MockInferenceEngine::long_bias().with_expected_feature_len(4);
        let mut exec = Executor::new(config, engine);
        let snap = snapshot(true, 50, 0.01, vec![0.1, 0.0, 0.01]); // len 3 != 4
        let (dec, order) = exec.on_snapshot(&snap);
        assert_eq!(dec.action, Action::NoTrade);
        assert!(dec.reason_codes.contains(&"meta_mismatch".to_string()));
        assert!(order.is_none());
    }

    #[test]
    fn inference_error_yields_no_trade_and_reason() {
        let config = ExecutorConfig::from_env_or_default();
        let engine = MockInferenceEngineFails::new();
        let mut exec = Executor::new(config, engine);
        let snap = snapshot(true, 50, 0.01, vec![0.1, 0.0, 0.01, 0.5]);
        let (dec, order) = exec.on_snapshot(&snap);
        assert_eq!(dec.action, Action::NoTrade);
        assert!(dec.reason_codes.contains(&"inference_error".to_string()));
        assert!(order.is_none());
    }
}

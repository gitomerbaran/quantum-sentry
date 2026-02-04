//! Scale-invariant feature computation: log returns, spread_pct, imbalance.
//! No raw price as primary feature; safe division (NOT_READY on invalid).

/// Tick-to-tick log return: ln(mid_t / mid_{t-1}).
/// Returns None if either mid is non-finite or <= 0.
pub fn log_return_1(mid_now: f64, mid_prev: f64) -> Option<f32> {
    if !mid_now.is_finite() || mid_now <= 0.0 || !mid_prev.is_finite() || mid_prev <= 0.0 {
        return None;
    }
    let r = (mid_now / mid_prev).ln();
    if r.is_finite() {
        Some(r as f32)
    } else {
        None
    }
}

/// Log return over window: ln(mid_t / mid_{t-N}).
pub fn log_return_window(mid_last: f64, mid_first: f64) -> Option<f32> {
    if !mid_last.is_finite() || mid_last <= 0.0 || !mid_first.is_finite() || mid_first <= 0.0 {
        return None;
    }
    let r = (mid_last / mid_first).ln();
    if r.is_finite() {
        Some(r as f32)
    } else {
        None
    }
}

/// Spread as fraction of mid: (ask - bid) / mid. Safe: None if mid <= 0 or non-finite.
pub fn spread_pct(bid_price: f64, ask_price: f64) -> Option<f32> {
    let mid = (bid_price + ask_price) / 2.0;
    if !mid.is_finite() || mid <= 0.0 {
        return None;
    }
    let spread = ask_price - bid_price;
    if !spread.is_finite() || spread < 0.0 {
        return None;
    }
    let pct = (spread / mid) as f32;
    if pct.is_finite() {
        Some(pct)
    } else {
        None
    }
}

/// Imbalance: (bid_qty - ask_qty) / (bid_qty + ask_qty). Safe: 0.0 when denominator is 0.
pub fn imbalance(bid_qty: f64, ask_qty: f64) -> f32 {
    let denom = bid_qty + ask_qty;
    if !denom.is_finite() || denom <= 0.0 {
        return 0.0;
    }
    let num = bid_qty - ask_qty;
    if !num.is_finite() {
        return 0.0;
    }
    let imb = (num / denom) as f32;
    if imb.is_finite() {
        imb
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn log_return_correctness() {
        // 100 -> 110 => ln(1.1)
        let r = log_return_1(110.0, 100.0).unwrap();
        let expected = 1.1f64.ln() as f32;
        assert_relative_eq!(r, expected, epsilon = 1e-5);
    }

    #[test]
    fn log_return_invalid_returns_none() {
        assert!(log_return_1(0.0, 100.0).is_none());
        assert!(log_return_1(100.0, 0.0).is_none());
        assert!(log_return_1(f64::NAN, 100.0).is_none());
    }

    #[test]
    fn spread_pct_correctness() {
        // bid=99 ask=101 => mid=100 spread_pct=0.02
        let s = spread_pct(99.0, 101.0).unwrap();
        assert_relative_eq!(s, 0.02f32, epsilon = 1e-5);
    }

    #[test]
    fn spread_pct_zero_mid_returns_none() {
        assert!(spread_pct(0.0, 0.0).is_none());
    }

    #[test]
    fn imbalance_correctness() {
        // bid_qty=3 ask_qty=1 => (3-1)/(3+1) = 0.5
        let imb = imbalance(3.0, 1.0);
        assert_relative_eq!(imb, 0.5f32, epsilon = 1e-5);
    }

    #[test]
    fn imbalance_zero_denom_returns_zero() {
        assert_eq!(imbalance(0.0, 0.0), 0.0);
    }
}

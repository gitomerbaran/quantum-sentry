use crate::model::norm_symbol;
use crate::wallet::Wallet;

#[test]
fn test_fee_logic() {
    let mut wallet = Wallet::new();
    wallet.usdt_balance = 2000.0;

    // Open long: margin 1000 USDT, leverage 1 => notional 1000, fee = notional * 0.0004 = 0.4.
    let price = 100.0;
    let margin = 1000.0;

    wallet.execute_buy_with_slippage("BTCUSDT", price, margin, Some(0.0));

    assert!((wallet.usdt_balance - 999.6).abs() < 1e-6);

    let sym = norm_symbol("BTCUSDT");
    let pos = wallet
        .positions
        .get(&sym)
        .unwrap_or_else(|| panic!("Position should exist"));
    assert!((pos.amount - 10.0).abs() < 1e-6);
}

#[test]
fn test_slippage_logic() {
    let mut wallet = Wallet::new();
    wallet.usdt_balance = 1000.0;

    let price = 100.0;
    let slippage = 0.0005;

    wallet.execute_buy_with_slippage("BTCUSDT", price, 100.0, Some(slippage));

    let sym = norm_symbol("BTCUSDT");
    let pos = wallet
        .positions
        .get(&sym)
        .unwrap_or_else(|| panic!("Position should exist"));
    let expected_exec_price = price * (1.0 + slippage);
    assert!((pos.average_entry_price - expected_exec_price).abs() < 1e-6);
    assert!(pos.average_entry_price > price);
}

#[test]
fn test_insufficient_funds() {
    let mut wallet = Wallet::new();
    wallet.usdt_balance = 9.0;

    wallet.execute_buy_with_slippage("BTCUSDT", 100.0, 100.0, Some(0.0));

    assert!(!wallet.has_position("BTCUSDT"));
    assert_eq!(wallet.usdt_balance, 9.0);
}

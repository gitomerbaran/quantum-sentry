use crate::wallet::Wallet;

#[test]
fn test_fee_logic() {
    let mut wallet = Wallet::new();
    wallet.usdt_balance = 2000.0;

    // Spend 1000 USDT. Fee should be 1.0 USDT.
    // Slippage = 0.0 for deterministic check.
    let price = 100.0;
    let spend_amount = 1000.0;

    wallet.execute_buy_with_slippage("BTCUSDT", price, spend_amount, Some(0.0));

    // Balance should decrease by exactly 1000.0
    assert_eq!(wallet.usdt_balance, 1000.0);

    // Position amount:
    // Net spend = 1000 - (1000 * 0.001) = 1000 - 1 = 999.
    // Exec price = 100.0.
    // Qty = 999 / 100 = 9.99.

    let pos = wallet
        .positions
        .get("BTCUSDT")
        .expect("Position should exist");
    assert!(
        (pos.amount - 9.99).abs() < 1e-6,
        "Amount should be 9.99, got {}",
        pos.amount
    );

    // Fee was 1.0. Implicitly verified by net spend being 999.
}

#[test]
fn test_slippage_logic() {
    let mut wallet = Wallet::new();
    wallet.usdt_balance = 1000.0;

    let price = 100.0;
    let slippage = 0.0005; // 0.05%

    wallet.execute_buy_with_slippage("BTCUSDT", price, 100.0, Some(slippage));

    let pos = wallet
        .positions
        .get("BTCUSDT")
        .expect("Position should exist");
    let expected_exec_price = price * (1.0 + slippage);

    // Check entry price is correct
    assert!((pos.average_entry_price - expected_exec_price).abs() < 1e-6);

    // Assert strictly greater (since slippage > 0)
    assert!(pos.average_entry_price > price);
}

#[test]
fn test_insufficient_funds() {
    let mut wallet = Wallet::new();
    wallet.usdt_balance = 9.0; // Less than 10.0

    wallet.execute_buy_with_slippage("BTCUSDT", 100.0, 100.0, Some(0.0));

    // Should not trade
    assert!(!wallet.has_position("BTCUSDT"));
    assert_eq!(wallet.usdt_balance, 9.0);
}

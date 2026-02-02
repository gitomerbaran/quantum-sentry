import pytest
import pandas as pd
import numpy as np
import torch
import sys
import os
import logging

# Add scripts to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from scripts.prepare_tcn_data import process_data

def test_process_data_shapes_and_logic():
    # 1. Create Mock Data (100 rows)
    n_rows = 100
    mid_prices = np.arange(100, 100 + n_rows, dtype=float)

    # Bids/Asks
    bids = mid_prices - 0.1
    asks = mid_prices + 0.1
    qty = 1.0

    data = {
        'ts_exchange': pd.date_range(start='2024-01-01', periods=n_rows, freq='100ms'),
        'bids_price': [[b] for b in bids],
        'asks_price': [[a] for a in asks],
        'bids_qty': [[qty] for _ in range(n_rows)],
        'asks_qty': [[qty] for _ in range(n_rows)]
    }

    df = pd.DataFrame(data)

    # 2. Process Data
    lookback = 60
    horizon = 10
    X, y = process_data(df, lookback=lookback, forecast_horizon=horizon)

    # 3. Verify Shapes
    # Expected N: 100 - horizon(10) - lookback(60) + 1 = 31?
    # Valid windows end at indices: lookback-1 to len(df)-horizon-1
    # 59 to 89. Count = 89 - 59 + 1 = 31.
    expected_N = 31
    assert X.shape == (expected_N, 4, lookback)
    assert y.shape == (expected_N,)

def test_process_data_insufficient_data():
    df = pd.DataFrame({
        'bids_price': [[100.0]] * 50,
        'asks_price': [[101.0]] * 50,
        'bids_qty': [[1.0]] * 50,
        'asks_qty': [[1.0]] * 50
    })
    # Add scalar columns needed if code checks them before array parsing?
    # The current code parses arrays first.

    with pytest.raises(ValueError, match="Not enough data"):
        process_data(df, lookback=60, forecast_horizon=10)

def test_process_data_empty_arrays():
    # Scenario: Some rows have empty arrays for bids/asks (e.g. data gap)
    n_rows = 100
    mid_prices = np.arange(100, 100 + n_rows, dtype=float)

    bids_price = [[p - 0.1] for p in mid_prices]
    asks_price = [[p + 0.1] for p in mid_prices]
    bids_qty = [[1.0] for _ in range(n_rows)]
    asks_qty = [[1.0] for _ in range(n_rows)]

    # Introduce empty arrays at index 70
    bids_price[70] = []
    asks_price[70] = []

    df = pd.DataFrame({
        'bids_price': bids_price,
        'asks_price': asks_price,
        'bids_qty': bids_qty,
        'asks_qty': asks_qty
    })

    # The code should drop these rows.
    # Original length 100. One row dropped -> 99.
    # Lookback 60, Horizon 10.
    # Valid indices in cleaned df (0..98):
    # Window ends: 59 .. 98-10 = 88.
    # Count: 88 - 59 + 1 = 30.

    X, y = process_data(df, lookback=60, forecast_horizon=10)

    assert X.shape[0] == 30
    # Ensure no NaNs in X
    assert not torch.isnan(X).any(), "Found NaNs in X after processing empty arrays"

def test_process_data_nan_handling():
    # Scenario: Input data results in NaNs (e.g. 0 qty -> div by zero? handled by code?)
    # Or explicitly passing NaNs in arrays if that's possible, or just NaNs in constructed features.

    n_rows = 100
    # Row 50 has 0 qty for bid and ask -> Imbalance calculation might produce NaN if not handled
    bids_qty = [[1.0]] * n_rows
    asks_qty = [[1.0]] * n_rows
    bids_qty[50] = [0.0]
    asks_qty[50] = [0.0]

    df = pd.DataFrame({
        'bids_price': [[100.0]] * n_rows,
        'asks_price': [[101.0]] * n_rows,
        'bids_qty': bids_qty,
        'asks_qty': asks_qty
    })

    # Imbalance = (Bid - Ask) / (Bid + Ask) -> (0 - 0) / (0 + 0) -> NaN if not handled.
    # Depth pressure similarly.

    # Note: Current code does: denominator.replace(0, 1) for depth pressure.
    # For imbalance: (df['best_bid_qty'] - df['best_ask_qty']) / (df['best_bid_qty'] + df['best_ask_qty'])
    # If both are 0, this will be 0/0 = NaN.
    # The test expects NO NaNs in X.

    try:
        X, y = process_data(df, lookback=60, forecast_horizon=10)
        assert not torch.isnan(X).any(), "Found NaNs in X due to 0 quantity"
    except Exception as e:
        # If it raises due to NaN check (if implemented), that's one thing, but we want it to handle it.
        # If the code presently generates NaNs, this test will FAIL, which is what we want (Audit).
        # We will assert that X has no nans.
        pytest.fail(f"Process data failed or produced error: {e}")

def test_leakage_verification():
    """
    Verify strictly that X at time t does NOT contain information from t+1.
    """
    lookback = 10
    horizon = 1
    n_rows = 50

    # Flat price 100
    mid_prices = np.full(n_rows, 100.0)

    # At index 20 (row 20), price jumps to 200.0
    # This means at time t=19 (window ending at 19), price is 100.
    # At time t=20 (window ending at 20), price is 200.
    mid_prices[20:] = 200.0

    bids = mid_prices - 1.0
    asks = mid_prices + 1.0
    qty = 1.0

    df = pd.DataFrame({
        'bids_price': [[b] for b in bids],
        'asks_price': [[a] for a in asks],
        'bids_qty': [[qty] for _ in range(n_rows)],
        'asks_qty': [[qty] for _ in range(n_rows)]
    })

    X, y = process_data(df, lookback=lookback, forecast_horizon=horizon)

    # Windows:
    # X[i] uses rows i to i+lookback-1. (Wait, let's check sliding_window_view logic in code)
    # The code says:
    # windows = sliding_window_view(feature_data, window_shape=lookback, axis=0)
    # If feature_data is [0, 1, 2, ...], window_shape=2
    # windows[0] = [0, 1]
    # windows[1] = [1, 2]
    # "The 'current time' for window i is its last row index."
    # So window[i] ends at index `lookback - 1 + i`.

    # We want the window that ends exactly at row 19 (index 19).
    # row 19 has price 100. row 20 has price 200.
    # This window should NOT see 200.

    # Window end index = lookback - 1 + i
    # We want lookback - 1 + i = 19
    # 10 - 1 + i = 19 => 9 + i = 19 => i = 10.

    # Let's verify X[10]
    # It corresponds to window rows [10, 11, ..., 19]. All should be ~100.

    # Feature 0 is mid_price.
    # X shape: (N, Channels, Lookback)

    if 10 < len(X):
        last_price_in_window = X[10, 0, -1].item() # Channel 0, last time step
        assert np.isclose(last_price_in_window, 100.0), f"Leakage! Expected 100, got {last_price_in_window}"

        # Now check window ending at 20 (index 11)
        # It should see the jump at the very last step.
        last_price_next_window = X[11, 0, -1].item()
        assert np.isclose(last_price_next_window, 200.0), f"Expected 200, got {last_price_next_window}"

        # Check target for window ending at 19 (index 10)
        # Target is log return from t (19) to t+horizon (20).
        # Price[19]=100, Price[20]=200. Return = ln(200/100) = ln(2) approx 0.693
        expected_target = np.log(200.0 / 100.0)
        assert np.isclose(y[10].item(), expected_target, atol=1e-5)
    else:
        pytest.fail("Not enough windows generated to test leakage point.")

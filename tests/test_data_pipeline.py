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

# Configure logging to suppress warnings during tests
logging.basicConfig(level=logging.CRITICAL)

def test_process_data_shapes_and_logic():
    """Verifies output shapes and correct target alignment."""
    # 1. Create Mock Data (100 rows)
    # Price increases by 1.0 each tick: 100, 101, ..., 199
    n_rows = 100
    mid_prices = np.arange(100, 100 + n_rows, dtype=float)

    # Bids/Asks
    # Mid = (Bid + Ask) / 2
    # Spread = 0.2
    # Bid = Mid - 0.1, Ask = Mid + 0.1
    bids = mid_prices - 0.1
    asks = mid_prices + 0.1

    # Qty
    qty = 1.0

    data = {
        'ts_exchange': pd.date_range(start='2024-01-01', periods=n_rows, freq='100ms'),
        'bids_price': [[b] for b in bids], # Array(Float64)
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
    # Expected N:
    # valid_start = 59 (since lookback=60, indices 0..59 is first window)
    # valid_end: last index i such that i + horizon < 100
    # i < 90. So max i = 89.
    # Windows end at indices: 59, 60, ..., 89.
    # Count: 89 - 59 + 1 = 31

    expected_N = 31
    assert X.shape == (expected_N, 4, lookback)
    assert y.shape == (expected_N,)

    # 4. Leakage & Correctness Test
    # Check first sample (window ending at row 59)
    # Target should be LogReturn(Price[69] / Price[59])

    price_t = mid_prices[59] # 159.0
    price_t_plus_10 = mid_prices[69] # 169.0
    expected_target = np.log(price_t_plus_10 / price_t)

    actual_target = y[0].item()

    assert np.isclose(actual_target, expected_target, atol=1e-6), \
        f"Target mismatch! Expected {expected_target}, got {actual_target}"

    # Check input feature correctness for last step in window
    # X[0] shape is (4, 60). Last column is X[0, :, 59] which corresponds to row 59.
    # Feature 0 is MidPrice.
    assert np.isclose(X[0, 0, -1].item(), price_t, atol=1e-6)

def test_process_data_empty_arrays():
    """Verifies that rows with empty arrays are handled (dropped or padded)."""
    # 65 rows. 5 rows in the middle have empty arrays.
    n_rows = 70
    mid_prices = np.full(n_rows, 100.0)

    bids = [[x] for x in mid_prices]
    asks = [[x] for x in mid_prices]
    qtys = [[1.0] for _ in range(n_rows)]

    # Introduce empty arrays at indices 30-34
    for i in range(30, 35):
        bids[i] = []
        asks[i] = []

    data = {
        'ts_exchange': pd.date_range(start='2024-01-01', periods=n_rows, freq='100ms'),
        'bids_price': bids,
        'asks_price': asks,
        'bids_qty': qtys,
        'asks_qty': qtys
    }
    df = pd.DataFrame(data)

    # With lookback 60, we need contiguous valid data?
    # The current logic drops rows with NaNs (parsed from empty arrays).
    # If rows 30-34 are dropped, we have a gap in time.
    # The simple sliding window on the REDUCED dataframe will concatenate row 29 and row 35.
    # This might be scientifically debatable (time jump), but for now we verify it doesn't crash
    # and produces output if enough data remains.
    # 70 rows - 5 bad = 65 valid rows.
    # Lookback 60. Horizon 10.
    # Max index valid for target: 65 - 10 = 55.
    # Start index: 59.
    # Wait: 59 > 55. So valid_end_index (55) <= valid_start_index (59).
    # Raise ValueError "Not enough data".

    # Let's provide MORE data so we survive the drop.
    # 100 rows. 5 bad. 95 valid.
    # Lookback 60. Horizon 10.
    # valid_end = 95 - 10 = 85.
    # start = 59.
    # Indices 59..84. Count = 26 windows.

    n_rows_large = 100
    mid_prices_large = np.full(n_rows_large, 100.0)
    bids_large = [[x] for x in mid_prices_large]
    asks_large = [[x] for x in mid_prices_large]
    qtys_large = [[1.0] for _ in range(n_rows_large)]

    for i in range(30, 35):
        bids_large[i] = []

    df_large = pd.DataFrame({
        'ts_exchange': pd.date_range(start='2024-01-01', periods=n_rows_large, freq='100ms'),
        'bids_price': bids_large,
        'asks_price': asks_large,
        'bids_qty': qtys_large,
        'asks_qty': qtys_large
    })

    X, y = process_data(df_large, lookback=60, forecast_horizon=10)

    # Check no NaNs
    assert torch.isnan(X).sum() == 0
    assert torch.isnan(y).sum() == 0
    assert len(X) > 0

def test_feature_leakage_strict():
    """
    Verifies that a future price jump does not leak into current features.
    Scenario:
    t=0..59: Price = 100
    t=60: Price jumps to 200
    Window ending at t=59 (index 0) should have MidPrice=100 in its last step.
    Target for t=59 (horizon 1) should reflect the jump (return ln(200/100)).
    """
    n_rows = 100
    # Price is 100 until index 59. Index 60 becomes 200.
    mid_prices = np.concatenate([np.full(60, 100.0), np.full(40, 200.0)])

    bids = [[x - 0.1] for x in mid_prices]
    asks = [[x + 0.1] for x in mid_prices]
    qtys = [[1.0] for _ in range(n_rows)]

    df = pd.DataFrame({
        'ts_exchange': pd.date_range(start='2024-01-01', periods=n_rows, freq='100ms'),
        'bids_price': bids,
        'asks_price': asks,
        'bids_qty': qtys,
        'asks_qty': qtys
    })

    # Lookback 60. Horizon 1.
    # First window: rows 0..59.
    # Last row of first window is 59. Price is 100.
    # Target is return from 59 to 60. Price(60)=200.

    X, y = process_data(df, lookback=60, forecast_horizon=1)

    # X shape: (N, 4, 60)
    # Check first window (index 0)
    # Last time step in window is index -1 (relative to lookback dim)
    # Feature 0 is MidPrice
    last_price_in_window = X[0, 0, -1].item()

    assert np.isclose(last_price_in_window, 100.0, atol=1e-6), \
        f"Leakage detected! Feature saw {last_price_in_window} instead of 100.0"

    # Check target
    # ln(200/100) = ln(2) approx 0.693
    expected_target = np.log(200.0 / 100.0)
    actual_target = y[0].item()

    assert np.isclose(actual_target, expected_target, atol=1e-6), \
        f"Target mismatch! Expected {expected_target}, got {actual_target}"

def test_process_data_insufficient_data():
    """Verifies ValueError is raised when data is insufficient."""
    # Only 50 rows, lookback 60
    df = pd.DataFrame({
        'bids_price': [[100.0]] * 50,
        'asks_price': [[101.0]] * 50,
        'bids_qty': [[1.0]] * 50,
        'asks_qty': [[1.0]] * 50
    })

    with pytest.raises(ValueError, match="Not enough data"):
        process_data(df, lookback=60, forecast_horizon=10)

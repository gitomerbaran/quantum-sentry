import pytest
import pandas as pd
import numpy as np
import torch
import sys
import os

# Add scripts to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from scripts.prepare_tcn_data import process_data

def test_process_data_shapes_and_logic():
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
    # valid_start = 59
    # limit = 100 - 10 = 90
    # indices: 59, 60, ..., 89
    # Count: 89 - 59 + 1 = 31

    expected_N = 31
    assert X.shape == (expected_N, 4, lookback)
    assert y.shape == (expected_N,)

    # 4. Leakage & Correctness Test
    # Check first sample (index 0)
    # Window ends at row 59.
    # Target should be LogReturn(Price[69] / Price[59])

    price_t = mid_prices[59] # 159.0
    price_t_plus_10 = mid_prices[69] # 169.0
    expected_target = np.log(price_t_plus_10 / price_t)

    # y[0] is a tensor, convert to float
    actual_target = y[0].item()

    assert np.isclose(actual_target, expected_target, atol=1e-6), \
        f"Target mismatch! Expected {expected_target}, got {actual_target}"

    # Check input feature correctness for last step in window
    # X[0] shape is (4, 60). Last column is X[0, :, 59] which corresponds to row 59.
    # Feature 0 is MidPrice.
    assert np.isclose(X[0, 0, -1].item(), price_t, atol=1e-6)

    # Check shift logic
    # X[1] corresponds to window ending at row 60.
    # Its target should be return(70 vs 60).
    price_t_next = mid_prices[60]
    price_t_next_plus_10 = mid_prices[70]
    expected_target_next = np.log(price_t_next_plus_10 / price_t_next)

    assert np.isclose(y[1].item(), expected_target_next, atol=1e-6)

def test_process_data_insufficient_data():
    # Only 50 rows, lookback 60
    df = pd.DataFrame({
        'bids_price': [[100.0]] * 50,
        'asks_price': [[101.0]] * 50,
        'bids_qty': [[1.0]] * 50,
        'asks_qty': [[1.0]] * 50
    })

    with pytest.raises(ValueError, match="Not enough data"):
        process_data(df, lookback=60, forecast_horizon=10)

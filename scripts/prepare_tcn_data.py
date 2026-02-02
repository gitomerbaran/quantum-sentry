"""
Data processing pipeline for TCN model training.

Fetches L2 order book snapshots from ClickHouse, computes HFT features
(MidPrice, Imbalance, Spread, DepthPressure), and prepares
sliding-window tensors for training.

Strictly enforces anti-leakage policies: features at time t utilize data
only up to time t, while targets represent future returns.
"""

import clickhouse_driver
import pandas as pd
import torch
import numpy as np
import argparse
import os
import logging
from typing import Tuple, List, Optional, Any, Union
from numpy.lib.stride_tricks import sliding_window_view

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_data(
    host: str = 'localhost',
    symbol: str = 'BTCUSDT',
    limit: int = 100000
) -> pd.DataFrame:
    """
    Fetches depth snapshots from ClickHouse.

    Args:
        host: ClickHouse host address.
        symbol: Trading symbol (e.g., 'BTCUSDT').
        limit: Number of rows to fetch.

    Returns:
        pd.DataFrame: DataFrame containing `ts_exchange`, `bids_price`, `bids_qty`,
                      `asks_price`, `asks_qty`.

    Raises:
        ConnectionError: If connection to ClickHouse fails.
        RuntimeError: If query execution fails.
    """
    try:
        client = clickhouse_driver.Client(host=host)

        query = f"""
        SELECT
            ts_exchange,
            bids_price,
            bids_qty,
            asks_price,
            asks_qty
        FROM quantum.depth_snapshots
        WHERE symbol = '{symbol}'
        ORDER BY ts_exchange ASC
        LIMIT {limit}
        """

        logger.info(f"Connecting to ClickHouse at {host} for {symbol}...")
        data, columns = client.execute(query, with_column_types=True)

        if not data:
            logger.warning(f"No data found for {symbol}.")
            return pd.DataFrame()

        df = pd.DataFrame(data, columns=[col[0] for col in columns])
        logger.info(f"Successfully fetched {len(df)} rows.")
        return df

    except Exception as e:
        logger.error(f"Failed to fetch data from ClickHouse: {e}")
        # In a production pipeline, we might want to retry or bubble up.
        # Here we bubble up as a RuntimeError to stop the pipeline.
        raise RuntimeError(f"Data fetch failed: {e}")

def calculate_depth_pressure(bids_qty: pd.Series, asks_qty: pd.Series) -> pd.Series:
    """
    Calculates depth pressure from full arrays.

    Formula:
        Pressure = (Total Bid Qty - Total Ask Qty) / (Total Bid Qty + Total Ask Qty)

    Args:
        bids_qty: Series of List[float], representing bid quantities at multiple levels.
        asks_qty: Series of List[float], representing ask quantities at multiple levels.

    Returns:
        pd.Series: Calculated depth pressure (float).
    """
    # Vectorized sum of list columns is tricky.
    # List comprehension is significantly faster than .apply(np.sum) for small lists.

    total_bid = pd.Series([sum(x) if isinstance(x, (list, np.ndarray)) else 0.0 for x in bids_qty])
    total_ask = pd.Series([sum(x) if isinstance(x, (list, np.ndarray)) else 0.0 for x in asks_qty])

    # Avoid division by zero
    denominator = total_bid + total_ask
    # Replace 0 with 1 to avoid NaN (0/0 -> NaN, x/0 -> Inf)
    denominator = denominator.replace(0.0, 1.0)

    return (total_bid - total_ask) / denominator

def process_data(
    df: pd.DataFrame,
    lookback: int = 60,
    forecast_horizon: int = 10
) -> Tuple[torch.Tensor, torch.Tensor]:
    """
    Processes the DataFrame into feature and target tensors.

    Features:
    1. MidPrice: (Best Bid + Best Ask) / 2
    2. Imbalance: (Best Bid Qty - Best Ask Qty) / (Best Bid Qty + Best Ask Qty)
    3. Spread: Best Ask - Best Bid
    4. DepthPressure: (Total Bid Qty - Total Ask Qty) / Total Qty

    Target:
    - LogReturn of MidPrice `forecast_horizon` ticks ahead.

    Args:
        df: Input DataFrame with raw ClickHouse data.
        lookback: Size of the sliding window for features.
        forecast_horizon: Number of ticks ahead to calculate target return.

    Returns:
        X: Feature Tensor of shape (Batch, Channels, Lookback).
        y: Target Tensor of shape (Batch,).

    Raises:
        ValueError: If insufficient data remains after processing.
    """
    if df.empty:
        raise ValueError("DataFrame is empty")

    logger.info("Starting feature engineering...")

    # 1. Parse Arrays to Scalars (L1)
    # Using list comprehensions for speed over .apply
    # Handle empty lists gracefully by returning NaN (to be dropped later)

    def safe_get_first(arr: Any) -> float:
        if isinstance(arr, (list, np.ndarray)) and len(arr) > 0:
            return float(arr[0])
        return np.nan

    df['best_bid'] = [safe_get_first(x) for x in df['bids_price']]
    df['best_ask'] = [safe_get_first(x) for x in df['asks_price']]
    df['best_bid_qty'] = [safe_get_first(x) for x in df['bids_qty']]
    df['best_ask_qty'] = [safe_get_first(x) for x in df['asks_qty']]

    initial_len = len(df)
    # Drop rows with missing L1 data (empty snapshots)
    df.dropna(subset=['best_bid', 'best_ask', 'best_bid_qty', 'best_ask_qty'], inplace=True)

    dropped_count = initial_len - len(df)
    if dropped_count > 0:
        logger.warning(f"Dropped {dropped_count} rows due to empty/malformed order book snapshots.")

    if df.empty:
        raise ValueError("No valid rows remaining after cleaning.")

    # 2. Calculate Features
    # Vectorized operations using Pandas/NumPy

    # MidPrice
    df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2.0

    # Imbalance (L1)
    bid_qty = df['best_bid_qty']
    ask_qty = df['best_ask_qty']
    # Avoid div by zero
    total_l1 = bid_qty + ask_qty
    total_l1 = total_l1.replace(0.0, 1.0)
    df['imbalance'] = (bid_qty - ask_qty) / total_l1

    # Spread
    df['spread'] = df['best_ask'] - df['best_bid']

    # Depth Pressure
    df['depth_pressure'] = calculate_depth_pressure(df['bids_qty'], df['asks_qty'])

    # Select Features
    feature_cols = ['mid_price', 'imbalance', 'spread', 'depth_pressure']

    # Ensure all features are float32
    feature_data = df[feature_cols].values.astype(np.float32)

    # Check for sufficient data BEFORE sliding window to avoid obscure errors
    if len(feature_data) < lookback:
         raise ValueError(f"Not enough data ({len(feature_data)} rows) for lookback ({lookback}).")

    # 3. Calculate Target
    # LogReturn of MidPrice 'forecast_horizon' ticks ahead.
    mid_price = df['mid_price']
    # shift(-H) moves t+H to t.
    future_price = mid_price.shift(-forecast_horizon)
    df['mid_price_log_return'] = np.log(future_price / mid_price)

    # 4. Prepare Sliding Windows
    # X[i] contains rows [i, i+lookback-1]
    # y[i] contains target from row [i+lookback-1]

    # axis=0 is time dimension.
    # windows shape: (num_windows, lookback, Channels) -> Wait, default is (N, W, C) or (N, C, W)?
    # sliding_window_view on (T, C) with window=L axis=0 produces (T-L+1, C, L).
    # Let's verify documentation/tests.
    # From tests output earlier: "Sliding window view of the array. The sliding window dimensions are inserted at the end"
    # Input: (T, C). Window axis 0.
    # Output: (T-L+1, C, L).
    # Example: shape (6, 2), window 3. -> (4, 2, 3).
    # Yes. (N, Channels, Lookback). This matches PyTorch Conv1d expectation.

    windows = sliding_window_view(feature_data, window_shape=lookback, axis=0)

    # Align windows with targets
    target_data = df['mid_price_log_return'].values.astype(np.float32)

    # The 'current time' for window i is its last row index.
    # Window 0 uses rows 0..L-1. Current time is L-1.
    num_windows = len(windows)

    # We need the target corresponding to the END of the window.
    # Indices in the original dataframe corresponding to window ends:
    window_end_indices = np.arange(lookback - 1, lookback - 1 + num_windows)

    # Filter out windows where the target is NaN (due to shifting)
    # Target is valid up to len(df) - forecast_horizon - 1
    valid_limit = len(df) - forecast_horizon
    valid_mask = window_end_indices < valid_limit

    if not np.any(valid_mask):
         raise ValueError("Not enough data to form complete windows with targets.")

    X = windows[valid_mask] # Shape: (N, Channels, Lookback)

    # Get corresponding targets
    target_indices = window_end_indices[valid_mask]
    y = target_data[target_indices]

    logger.info(f"Generated {len(X)} samples. X shape: {X.shape}, y shape: {y.shape}")

    return torch.from_numpy(X), torch.from_numpy(y)

def main():
    parser = argparse.ArgumentParser(description="Prepare TCN data from ClickHouse")
    parser.add_argument("--limit", type=int, default=100000, help="Number of rows to fetch")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Symbol to fetch")
    parser.add_argument("--output", type=str, default="data/tcn_dataset.pt", help="Output file path")
    parser.add_argument("--host", type=str, default="localhost", help="ClickHouse host")

    args = parser.parse_args()

    try:
        df = fetch_data(host=args.host, symbol=args.symbol, limit=args.limit)

        if df.empty:
            logger.warning("No data fetched. Exiting.")
            return

        X, y = process_data(df)

        # Ensure directory exists
        os.makedirs(os.path.dirname(args.output), exist_ok=True)

        torch.save({'X': X, 'y': y}, args.output)
        logger.info(f"Saved dataset to {args.output}")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()

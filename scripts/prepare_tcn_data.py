import clickhouse_driver
import pandas as pd
import torch
import numpy as np
import argparse
import os
from typing import Tuple

def fetch_data(host: str = 'localhost', symbol: str = 'BTCUSDT', limit: int = 100000) -> pd.DataFrame:
    """
    Fetches depth snapshots from ClickHouse.
    """
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

    print(f"Fetching data for {symbol} from ClickHouse...")
    data, columns = client.execute(query, with_column_types=True)
    df = pd.DataFrame(data, columns=[col[0] for col in columns])
    print(f"Fetched {len(df)} rows.")
    return df

def calculate_depth_pressure(bids_qty: pd.Series, asks_qty: pd.Series) -> pd.Series:
    """
    Calculates depth pressure from full arrays.
    Pressure = (Total Bid Qty - Total Ask Qty) / (Total Bid Qty + Total Ask Qty)
    """
    # Helper for row-wise sum
    def sum_arr(arr):
        return np.sum(arr)

    total_bid = bids_qty.apply(sum_arr)
    total_ask = asks_qty.apply(sum_arr)

    # Avoid division by zero
    denominator = total_bid + total_ask
    denominator = denominator.replace(0, 1) # If both are 0, pressure is 0

    return (total_bid - total_ask) / denominator

def process_data(df: pd.DataFrame, lookback: int = 60, forecast_horizon: int = 10) -> Tuple[torch.Tensor, torch.Tensor]:
    """
    Processes the DataFrame into features and targets.

    Features:
    - MidPrice
    - Imbalance (L1)
    - Spread (L1)
    - DepthPressure

    Target:
    - LogReturn 10 ticks ahead

    Returns:
    - X: Tensor of shape (N, Channels, Lookback)
    - y: Tensor of shape (N,)
    """
    if df.empty:
        raise ValueError("DataFrame is empty")

    # 1. Parse Arrays to Scalars (L1)
    # ClickHouse arrays come as lists in pandas
    df['best_bid'] = df['bids_price'].apply(lambda x: x[0] if len(x) > 0 else np.nan)
    df['best_ask'] = df['asks_price'].apply(lambda x: x[0] if len(x) > 0 else np.nan)
    df['best_bid_qty'] = df['bids_qty'].apply(lambda x: x[0] if len(x) > 0 else np.nan)
    df['best_ask_qty'] = df['asks_qty'].apply(lambda x: x[0] if len(x) > 0 else np.nan)

    # Drop rows with missing L1 data
    df = df.dropna(subset=['best_bid', 'best_ask', 'best_bid_qty', 'best_ask_qty'])

    # 2. Calculate Features
    # MidPrice
    df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2.0

    # Imbalance L1
    # (BidQty - AskQty) / (BidQty + AskQty)
    df['imbalance'] = (df['best_bid_qty'] - df['best_ask_qty']) / (df['best_bid_qty'] + df['best_ask_qty'])

    # Spread
    df['spread'] = df['best_ask'] - df['best_bid']

    # Depth Pressure
    df['depth_pressure'] = calculate_depth_pressure(df['bids_qty'], df['asks_qty'])

    features = ['mid_price', 'imbalance', 'spread', 'depth_pressure']
    feature_data = df[features].values.astype(np.float32)

    if len(feature_data) < lookback:
         raise ValueError("Not enough data for the given lookback and horizon.")

    # 3. Calculate Target
    # LogReturn of MidPrice 'forecast_horizon' ticks ahead.
    mid_price = df['mid_price']
    future_price = mid_price.shift(-forecast_horizon)
    df['target'] = np.log(future_price / mid_price)

    # 4. Prepare Sliding Windows
    # X[i] uses rows i-lookback+1 to i.
    # y[i] uses row i (which contains target derived from i+forecast_horizon).

    from numpy.lib.stride_tricks import sliding_window_view

    # Create windows of shape (num_windows, C, lookback)
    # axis=0 corresponds to the time dimension.
    windows = sliding_window_view(feature_data, window_shape=lookback, axis=0)

    # Align windows with targets
    target_data = df['target'].values.astype(np.float32)

    # The 'current time' for window i is its last row index.
    # Window index i starts at row i and ends at row i + lookback - 1.
    num_windows = len(windows)
    window_end_indices = np.arange(lookback - 1, lookback - 1 + num_windows)

    # Filter out windows where the target is NaN (due to shifting)
    valid_mask = window_end_indices < (len(df) - forecast_horizon)

    if not np.any(valid_mask):
         raise ValueError("Not enough data for the given lookback and horizon.")

    X = windows[valid_mask] # Shape: (N, Channels, Lookback)

    # Get corresponding targets
    target_indices = window_end_indices[valid_mask]
    y = target_data[target_indices]

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

        if len(df) < 100:
            print("Not enough data fetched to process.")
            return

        print("Processing data...")
        X, y = process_data(df)

        print(f"Data processed. X shape: {X.shape}, y shape: {y.shape}")

        # Ensure directory exists
        os.makedirs(os.path.dirname(args.output), exist_ok=True)

        torch.save({'X': X, 'y': y}, args.output)
        print(f"Saved to {args.output}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

import clickhouse_driver
import pandas as pd
import torch
import numpy as np
import argparse
import os
import logging
from typing import Tuple, List, Optional, Any
from clickhouse_driver.errors import Error as ClickHouseError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fetch_data(host: str = 'localhost', symbol: str = 'BTCUSDT', limit: int = 100000) -> pd.DataFrame:
    """
    Fetches depth snapshots from ClickHouse.

    Args:
        host (str): ClickHouse host address.
        symbol (str): Trading symbol (e.g., 'BTCUSDT').
        limit (int): Maximum number of rows to fetch.

    Returns:
        pd.DataFrame: DataFrame containing market depth data.
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

        logging.info(f"Fetching data for {symbol} from ClickHouse...")
        data, columns = client.execute(query, with_column_types=True)
        df = pd.DataFrame(data, columns=[col[0] for col in columns])
        logging.info(f"Fetched {len(df)} rows.")
        return df
    except ClickHouseError as e:
        logging.error(f"ClickHouse error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error fetching data: {e}")
        raise

def calculate_depth_pressure(bids_qty: pd.Series, asks_qty: pd.Series) -> pd.Series:
    """
    Calculates depth pressure from full arrays.
    Pressure = (Total Bid Qty - Total Ask Qty) / (Total Bid Qty + Total Ask Qty)

    Args:
        bids_qty (pd.Series): Series containing lists of bid quantities.
        asks_qty (pd.Series): Series containing lists of ask quantities.

    Returns:
        pd.Series: Calculated depth pressure.
    """
    # Use list comprehension for faster sum of lists in object column
    # Fallback to 0.0 if x is not iterable (though it should be a list)
    total_bid = pd.Series([sum(x) if isinstance(x, list) else 0.0 for x in bids_qty], index=bids_qty.index)
    total_ask = pd.Series([sum(x) if isinstance(x, list) else 0.0 for x in asks_qty], index=asks_qty.index)

    # Avoid division by zero
    denominator = total_bid + total_ask
    # Replace 0 with 1 to avoid ZeroDivisionError, result will be 0 numerator / 1 = 0
    denominator = denominator.replace(0, 1)

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

    Args:
        df (pd.DataFrame): Input DataFrame with raw depth data.
        lookback (int): Number of past ticks to include in each sample.
        forecast_horizon (int): Number of ticks ahead to predict return.

    Returns:
        Tuple[torch.Tensor, torch.Tensor]:
            - X: Feature tensor of shape (N, Channels, Lookback).
            - y: Target tensor of shape (N,).
    """
    if df.empty:
        raise ValueError("DataFrame is empty")

    try:
        # 1. Parse Arrays to Scalars (L1)
        # ClickHouse arrays come as lists in pandas
        # Use .str accessor which works on list-like object columns
        df['best_bid'] = df['bids_price'].str[0]
        df['best_ask'] = df['asks_price'].str[0]
        df['best_bid_qty'] = df['bids_qty'].str[0]
        df['best_ask_qty'] = df['asks_qty'].str[0]

        # Drop rows with missing L1 data (e.g. empty arrays resulted in NaN)
        initial_len = len(df)
        df = df.dropna(subset=['best_bid', 'best_ask', 'best_bid_qty', 'best_ask_qty'])
        if len(df) < initial_len:
            logging.warning(f"Dropped {initial_len - len(df)} rows due to missing L1 data.")

        # 2. Calculate Features
        # MidPrice
        df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2.0

        # Imbalance L1
        # (BidQty - AskQty) / (BidQty + AskQty)
        # Handle division by zero using fillna(0)
        imbalance_denom = df['best_bid_qty'] + df['best_ask_qty']
        imbalance_num = df['best_bid_qty'] - df['best_ask_qty']
        df['imbalance'] = (imbalance_num / imbalance_denom).fillna(0.0)

        # Spread
        df['spread'] = df['best_ask'] - df['best_bid']

        # Depth Pressure
        df['depth_pressure'] = calculate_depth_pressure(df['bids_qty'], df['asks_qty'])

        features = ['mid_price', 'imbalance', 'spread', 'depth_pressure']
        feature_data = df[features].values.astype(np.float32)

        # Check for any remaining NaNs/Infs
        if np.isnan(feature_data).any() or np.isinf(feature_data).any():
             logging.warning("Found NaNs or Infs in feature data after processing. Dropping affected rows.")
             mask = ~np.isnan(feature_data).any(axis=1) & ~np.isinf(feature_data).any(axis=1)
             feature_data = feature_data[mask]
             # Also filter df to align for target calculation
             df = df[mask]
             if len(df) == 0:
                 raise ValueError("All data dropped due to NaNs/Infs.")

        if len(feature_data) < lookback:
             raise ValueError("Not enough data for the given lookback and horizon.")

        # 3. Calculate Target
        # LogReturn of MidPrice 'forecast_horizon' ticks ahead.
        mid_price = df['mid_price']
        future_price = mid_price.shift(-forecast_horizon)
        df['mid_price_log_return'] = np.log(future_price / mid_price)

        # 4. Prepare Sliding Windows
        # X[i] uses rows i-lookback+1 to i.
        # y[i] uses row i (which contains target derived from i+forecast_horizon).

        from numpy.lib.stride_tricks import sliding_window_view

        # Create windows of shape (num_windows, C, lookback)
        # axis=0 corresponds to the time dimension.
        windows = sliding_window_view(feature_data, window_shape=lookback, axis=0)

        # Align windows with targets
        target_data = df['mid_price_log_return'].values.astype(np.float32)

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

    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

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
            logging.warning("Not enough data fetched to process.")
            return

        logging.info("Processing data...")
        X, y = process_data(df)

        logging.info(f"Data processed. X shape: {X.shape}, y shape: {y.shape}")

        # Ensure directory exists
        os.makedirs(os.path.dirname(args.output), exist_ok=True)

        torch.save({'X': X, 'y': y}, args.output)
        logging.info(f"Saved to {args.output}")

    except Exception as e:
        logging.error(f"Main execution failed: {e}")

if __name__ == "__main__":
    main()

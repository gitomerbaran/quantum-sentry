# Phase 2: AI Data Laboratory

## Tasks
- [x] Data Ingestion (Rust) - *Completed in Phase 1*
- [x] Data Pipeline Script (Python) - `scripts/prepare_tcn_data.py`
- [x] Unit Tests for Pipeline - `tests/test_data_pipeline.py`
- [ ] Model Training (Colab/A100)
- [ ] Rust Inference Integration

## Notes
- The data pipeline fetches `depth_snapshots` from ClickHouse.
- It calculates features: MidPrice, Imbalance, Spread, DepthPressure.
- It targets LogReturn 10 ticks ahead.
- Anti-leakage is strictly enforced via sliding windows and aligned targets.

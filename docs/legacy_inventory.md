# Legacy Inventory

Source: `../bortrace_data`

## Top level

- `txt/`: extracted text data, 2254 `.TXT` files observed
- `lzh/`: compressed source data, 2254 `.lzh` files observed
- `output_v4/`: old trained model and prediction output
- `archive/`: older experiments and scripts

## Active-looking scripts

- `downloader_v2.py`: download/extract flow
- `train_and_backtest_v4.py`: parser, feature engineering, training, backtest in one file
- `train_final_model_v4.py`: final model training
- `predict_all_day_v5.py`: all-day prediction
- `predict_live_v5.py`: live prediction/notification
- `analyze_calibration.py`: probability calibration check
- `test_target_detection.py`: target race detection check

## Initial concerns

- Old scripts use absolute paths such as `c:\Users\ryuou\.vscode\PyCode`.
- Parsing, feature engineering, training, betting simulation, and output are tightly coupled.
- Some source comments/strings appear mojibake in terminal output, so encoding handling needs care before porting logic.

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.transform_load import transform_data

@pytest.fixture
def sample_raw_data():
    """Provides a sample raw JSON payload for testing."""
    return {
        "TEST.WA": {
            "metadata": {"symbol": "TEST.WA"},
            "data": [
                {"Date": "2024-03-01", "Open": 100.0, "High": 105.0, "Low": 95.0, "Close": 102.0, "Volume": 1000},
                {"Date": "2024-03-02", "Open": None, "High": None, "Low": None, "Close": None, "Volume": None}, # Missing data simulation
                {"Date": "2024-03-03", "Open": 103.0, "High": 107.0, "Low": 101.0, "Close": 106.0, "Volume": 1500},
                {"Date": "2024-03-04", "Open": 106.0, "High": 110.0, "Low": 105.0, "Close": 109.0, "Volume": 2000}
            ]
        },
        "US.ETF": {
            "metadata": {"symbol": "US.ETF"},
            "data": [
                {"Date": "2024-03-01", "Open": 50.0, "High": 52.0, "Low": 49.0, "Close": 51.0, "Volume": 5000}
            ]
        }
    }

def test_transform_data_basic(sample_raw_data):
    """Test if transformation handles column renaming and basic types correctly."""
    df = transform_data(sample_raw_data)
    
    assert not df.empty
    assert len(df) == 5  # 4 from TEST.WA + 1 from US.ETF
    
    expected_columns = [
        'trade_date', 'open_price', 'high_price', 'low_price', 
        'close_price', 'volume', 'rolling_7d_avg_close', 
        'rolling_30d_avg_close', 'symbol', 'exchange', 'currency'
    ]
    for col in expected_columns:
        assert col in df.columns

def test_transform_data_missing_values(sample_raw_data):
    """Test if transformation correctly forward-fills missing data (weekends/holidays)."""
    df = transform_data(sample_raw_data)
    
    # Check the record for 2024-03-02 which had None initially.
    # It should be forward-filled from 2024-03-01 (Close: 102.0)
    test_wa_data = df[df['symbol'] == 'TEST.WA']
    record_03_02 = test_wa_data[test_wa_data['trade_date'] == '2024-03-02'].iloc[0]
    
    assert record_03_02['close_price'] == 102.0
    assert record_03_02['volume'] == 1000.0

def test_transform_data_rolling_averages(sample_raw_data):
    """Test if rolling averages are calculated properly."""
    df = transform_data(sample_raw_data)
    test_wa_data = df[df['symbol'] == 'TEST.WA']
    
    # 2024-03-01: 102.0 (avg: 102.0)
    # 2024-03-02: 102.0 (ffill) (avg: 102.0)
    # 2024-03-03: 106.0 (avg: 103.33)
    
    record_03_03 = test_wa_data[test_wa_data['trade_date'] == '2024-03-03'].iloc[0]
    expected_avg = (102.0 + 102.0 + 106.0) / 3
    
    assert round(record_03_03['rolling_7d_avg_close'], 2) == round(expected_avg, 2)

def test_transform_data_exchanges_and_currencies(sample_raw_data):
    """Test custom logic for assigning exchange and currency."""
    df = transform_data(sample_raw_data)
    
    test_wa = df[df['symbol'] == 'TEST.WA'].iloc[0]
    assert test_wa['exchange'] == 'GPW'
    assert test_wa['currency'] == 'PLN'
    
    test_us = df[df['symbol'] == 'US.ETF'].iloc[0]
    assert test_us['exchange'] == 'US_MARKET'
    assert test_us['currency'] == 'USD'
    
def test_transform_empty_data():
    """Test behavior when no data is provided."""
    empty_df = transform_data({})
    assert empty_df.empty

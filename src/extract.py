import os
import json
import logging
from datetime import datetime
import yfinance as yf
import boto3
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_stock_data(tickers, period="30d"):
    """
    Fetches historical stock market data using the yfinance library.
    """
    logger.info(f"Starting data extraction for tickers: {tickers} over period {period}.")
    raw_data = {}
    
    for ticker in tickers:
        try:
            logger.info(f"Fetching data for: {ticker}")
            stock = yf.Ticker(ticker)
            hist = stock.history(period=period)
            
            if hist.empty:
                logger.warning(f"No data found for ticker {ticker}.")
                continue
                
            # Reset index to make 'Date' a column and convert datetime to string for JSON serialization
            hist_reset = hist.reset_index()
            hist_reset['Date'] = hist_reset['Date'].dt.strftime('%Y-%m-%d')
            
            # Convert DataFrame to list of dictionaries (records-oriented JSON)
            records = hist_reset.to_dict(orient='records')
            
            # Append metadata (additional layer on extraction phase)
            raw_data[ticker] = {
                "metadata": {
                    "symbol": ticker,
                    "extracted_at": datetime.now().isoformat(),
                    "records_count": len(records)
                },
                "data": records
            }
            logger.info(f"Successfully fetched {len(records)} records for {ticker}.")
            
        except Exception as e:
            logger.error(f"Error while fetching data for {ticker}: {e}")
            
    return raw_data

def save_to_s3(data, bucket_name, execution_id):
    """
    Saves raw data as JSON format to AWS S3 bucket.
    """
    logger.info(f"Saving raw data to S3 bucket: {bucket_name}")
    s3_client = boto3.client('s3')
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    file_name = f"raw_stock_data/date={current_date}/data_{execution_id}.json"
    
    try:
        json_data = json.dumps(data, indent=2)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_data,
            ContentType='application/json'
        )
        logger.info(f"Data successfully saved to s3://{bucket_name}/{file_name}")
    except Exception as e:
        logger.error(f"Error while saving to S3: {e}")
        raise

if __name__ == "__main__":
    # Load dotenv variables (useful for local development)
    load_dotenv()
    
    BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    if not BUCKET_NAME:
        logger.error("Environment variable 'S3_BUCKET_NAME' is not set!")
        exit(1)
        
    # Execution identifier (using timestamp)
    execution_id = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # List of tickers to track (Polish companies + global ETF)
    # CDR.WA = CD Projekt, PKO.WA = PKO BP, VWCE.DE = Vanguard FTSE All-World UCITS ETF
    TICKERS = ["CDR.WA", "PKO.WA", "VWCE.DE", "^SPX"]
    
    logger.info(f"Starting extraction pipeline. Execution ID: {execution_id}")
    
    # 1. Fetch data
    extracted_data = fetch_stock_data(TICKERS, period="30d")
    
    if not extracted_data:
        logger.error("Failed to extract any data. Terminating pipeline.")
        exit(1)
        
    # 2. Save raw data to S3 Data Lake
    save_to_s3(extracted_data, BUCKET_NAME, execution_id)
    
    logger.info("Extraction phase completed successfully.")

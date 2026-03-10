import os
import json
import logging
import pandas as pd
from datetime import datetime
import boto3
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_latest_s3_file(bucket_name, prefix="raw_stock_data/"):
    """
    Finds the most recently created file in the given S3 bucket prefix.
    """
    s3_client = boto3.client('s3')
    logger.info(f"Looking for the latest file in s3://{bucket_name}/{prefix}")
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            logger.error("No files found in the specified S3 path.")
            return None
            
        # Sort objects by LastModified
        sorted_objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
        latest_file = sorted_objects[0]['Key']
        logger.info(f"Latest file identified: {latest_file}")
        return latest_file
    except Exception as e:
        logger.error(f"Error while listing S3 objects: {e}")
        return None

def download_data_from_s3(bucket_name, object_key):
    """
    Downloads and parses JSON data from S3.
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"Successfully downloaded and retrieved JSON data from {object_key}")
        return data
    except Exception as e:
        logger.error(f"Error while downloading {object_key} from S3: {e}")
        return None

def transform_data(raw_data):
    """
    Transforms raw JSON data into a clean Pandas DataFrame suitable for database insertion.
    Features: dropping missing values, standardizing dates, calculating rolling averages.
    """
    logger.info("Starting data transformation process...")
    processed_records = []
    
    for ticker, payload in raw_data.items():
        data_list = payload.get('data', [])
        if not data_list:
            logger.warning(f"No data points for {ticker}, skipping transformation.")
            continue
            
        df = pd.DataFrame(data_list)
        
        # Data Cleaning
        logger.info(f"Cleaning data for {ticker}...")
        
        # Ensure 'Date' exists and is standardized
        if 'Date' not in df.columns:
            logger.error(f"'Date' column missing in raw JSON for {ticker}.")
            continue
            
        # Rename columns to match PostgreSQL schema 
        df = df.rename(columns={
            'Date': 'trade_date',
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price',
            'Volume': 'volume'
        })
        
        # Drop irrelevant columns if any (e.g. Dividends, Stock Splits)
        cols_to_keep = ['trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
        df = df[[col for col in cols_to_keep if col in df.columns]]
        
        # Handle Missing Values (Forward fill, then drop remaining)
        df_cleaned = df.ffill().dropna()
        
        # Ensure numeric types
        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
        for col in numeric_cols:
            if col in df_cleaned.columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col])
                
        # Sort by date before calculating rolling averages
        df_cleaned = df_cleaned.sort_values(by='trade_date').reset_index(drop=True)
        
        # Data Enhancement: Calculate Rolling Averages
        df_cleaned['rolling_7d_avg_close'] = df_cleaned['close_price'].rolling(window=7, min_periods=1).mean()
        df_cleaned['rolling_30d_avg_close'] = df_cleaned['close_price'].rolling(window=30, min_periods=1).mean()
        
        # Add metadata fields
        df_cleaned['symbol'] = ticker
        
        # Basic logic for exchange and currency identification
        if ticker.endswith('.WA'):
            df_cleaned['exchange'] = 'GPW'
            df_cleaned['currency'] = 'PLN'
        elif ticker.endswith('.DE'):
            df_cleaned['exchange'] = 'XETRA'
            df_cleaned['currency'] = 'EUR'
        else:
            df_cleaned['exchange'] = 'US_MARKET'
            df_cleaned['currency'] = 'USD'
            
        processed_records.append(df_cleaned)
        logger.info(f"Successfully transformed {len(df_cleaned)} records for {ticker}.")

    if not processed_records:
        return pd.DataFrame()
        
    final_df = pd.concat(processed_records, ignore_index=True)
    logger.info(f"Transformation complete. Total records to be loaded: {len(final_df)}")
    return final_df

def log_audit_start(engine, execution_id, job_name):
    """
    Inserts a 'STARTED' record into the etl_audit_log table.
    """
    try:
        query = f"""
            INSERT INTO etl_audit_log (execution_id, job_name, status)
            VALUES ('{execution_id}', '{job_name}', 'STARTED')
            RETURNING log_id;
        """
        with engine.connect() as conn:
            result = conn.execute(text(query))
            log_id = result.scalar()
            conn.commit()
            return log_id
    except Exception as e:
        logger.error(f"Failed to insert audit START log: {e}")
        return None

def log_audit_end(engine, log_id, status, error_message=None, records_processed=0):
    """
    Updates the etl_audit_log table upon job completion.
    """
    if not log_id:
        return
        
    err_msg = f"'{error_message}'" if error_message else "NULL"
    try:
        query = f"""
            UPDATE etl_audit_log
            SET status = '{status}',
                error_message = {err_msg},
                records_processed = {records_processed},
                end_time = CURRENT_TIMESTAMP,
                duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time))
            WHERE log_id = {log_id};
        """
        with engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to update audit END log: {e}")

def load_data_to_postgres(df, engine, execution_id):
    """
    Loads transformed DataFrame into the PostgreSQL 'daily_prices_fact' table using an upsert logic.
    """
    if df.empty:
        logger.warning("Empty DataFrame provided. Nothing to load.")
        return 0

    log_id = log_audit_start(engine, execution_id, "transform_load_pipeline")
    records_inserted = 0
    
    try:
        # Convert df back to dictionaries for insert mapping
        records = df.to_dict(orient='records')
        
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Very simple "UPSERT" using alchemy core connection (skipping full ORM for simplicity in script)
        # PostgreSQL supports ON CONFLICT DO NOTHING
        with engine.connect() as conn:
            for record in records:
                try:
                    # Creating parameterized query to avoid SQL injection
                    columns = record.keys()
                    placeholders = ', '.join([f":{col}" for col in columns])
                    columns_str = ', '.join(columns)
                    
                    query = f"INSERT INTO daily_prices_fact ({columns_str}) VALUES ({placeholders}) ON CONFLICT (symbol, trade_date) DO NOTHING"
                    result = conn.execute(text(query), record)
                    records_inserted += result.rowcount
                except IntegrityError:
                    pass
            
            conn.commit()
            
        logger.info(f"Database load complete. {records_inserted} new records inserted.")
        log_audit_end(engine, log_id, "SUCCESS", records_processed=records_inserted)
        return records_inserted
        
    except Exception as e:
        logger.error(f"Error occurred during PostgreSQL database load: {e}")
        log_audit_end(engine, log_id, "FAILED", error_message=str(e)[:500])
        raise

if __name__ == "__main__":
    load_dotenv()
    
    # Configuration
    S3_BUCKET = os.getenv("S3_BUCKET_NAME")
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME", "etl_demo_db")
    
    if not all([S3_BUCKET, DB_HOST, DB_USER, DB_PASS]):
        logger.error("Missing required environment variables (S3_BUCKET_NAME, DB_HOST, DB_USER, DB_PASSWORD).")
        exit(1)
        
    execution_id = datetime.now().strftime("%Y%m%d%H%M%S")
    logger.info(f"Initiating Transform & Load pipeline. Execution ID: {execution_id}")
    
    try:
        # DB Engine Setup
        connection_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}?sslmode=require"
        engine = create_engine(connection_url)
        
        # 1. Fetch Latest Data from Data Lake
        latest_file_key = get_latest_s3_file(S3_BUCKET)
        if not latest_file_key:
            logger.error("Could not find any data files in S3.")
            exit(1)
            
        raw_json_data = download_data_from_s3(S3_BUCKET, latest_file_key)
        
        # 2. Transform the Data (Pandas)
        transformed_df = transform_data(raw_json_data)
        
        # 3. Load into Data Warehouse (PostgreSQL)
        load_data_to_postgres(transformed_df, engine, execution_id)
        
        logger.info("Transform & Load executed successfully!")
        
    except Exception as e:
        logger.critical(f"Pipeline failed with critical error: {e}")
        exit(1)

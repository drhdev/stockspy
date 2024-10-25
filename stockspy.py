"""
Filename: stockspy.py
Version: 2.0
Author: drhdev
Description: This script fetches stock data from the Twelve Data API, stores it in a MySQL database using SQLAlchemy,
calculates various financial indicators, and stores the calculations back into the database.
It is now fully MySQL powered.
"""

import logging
from logging.handlers import RotatingFileHandler
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import time
from datetime import datetime
import numpy as np
from sqlalchemy import create_engine, Column, String, Float, BigInteger, DateTime, Table, MetaData, select, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.url import URL

# Load environment variables
load_dotenv()

# User-configurable settings
# MySQL Database configuration from .env file
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT', '3306')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

# API Key from .env file
API_KEY = os.getenv('API_KEY')

# File paths
LOG_FILE_PATH = os.getenv('LOG_FILE_PATH', 'stockspy.log')

# Set up logging (overwrite old log file)
log_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler = logging.FileHandler(LOG_FILE_PATH, mode='w')
handler.setFormatter(log_formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Set up SQLAlchemy engine and metadata
database_url = URL.create(
    "mysql+mysqlconnector",
    username=MYSQL_USER,
    password=MYSQL_PASSWORD,
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    database=MYSQL_DATABASE,
)

engine = create_engine(database_url)
metadata = MetaData()

# Define database tables with 'ss_' prefix
ss_symbols = Table('ss_symbols', metadata,
                   Column('symbol', String(10), primary_key=True),
                   Column('created_at', DateTime))

ss_data = Table('ss_data', metadata,
                Column('symbol', String(10), primary_key=True),
                Column('datetime', DateTime, primary_key=True),
                Column('open', Float),
                Column('high', Float),
                Column('low', Float),
                Column('close', Float),
                Column('volume', BigInteger),
                Column('previous_close', Float),
                Column('created_at', DateTime))

ss_calculations = Table('ss_calculations', metadata,
                        Column('symbol', String(10), primary_key=True),
                        Column('datetime', DateTime, primary_key=True),
                        Column('percentage_diff', Float),
                        Column('sma_5', Float),
                        Column('sma_9', Float),
                        Column('sma_10', Float),
                        Column('sma_20', Float),
                        Column('sma_50', Float),
                        Column('sma_100', Float),
                        Column('sma_200', Float),
                        Column('sma_500', Float),
                        Column('bollinger_upper', Float),
                        Column('bollinger_lower', Float),
                        Column('percent_b', Float),
                        Column('macd', Float),
                        Column('macd_signal', Float),
                        Column('rsi', Float),
                        Column('ema_9', Float),
                        Column('calculated_at', DateTime))


def create_database():
    """
    Creates the necessary tables in the MySQL database if they do not exist.
    Inserts default symbols into 'ss_symbols' if the table was just created.
    """
    try:
        metadata.create_all(engine)
        conn = engine.connect()

        # Check if 'ss_symbols' table is empty
        s = select([ss_symbols])
        result = conn.execute(s).first()
        if result is None:
            # Insert default symbols
            default_symbols = [
                {'symbol': 'SPY', 'created_at': datetime.now()},
                {'symbol': 'VOO', 'created_at': datetime.now()},
                {'symbol': 'VTI', 'created_at': datetime.now()},
                {'symbol': 'AAPL', 'created_at': datetime.now()},
                {'symbol': 'MSFT', 'created_at': datetime.now()},
                {'symbol': 'GOOGL', 'created_at': datetime.now()},
                {'symbol': 'AMZN', 'created_at': datetime.now()},
                {'symbol': 'TSLA', 'created_at': datetime.now()},
                {'symbol': 'FB', 'created_at': datetime.now()},
                {'symbol': 'NFLX', 'created_at': datetime.now()}
            ]
            conn.execute(ss_symbols.insert(), default_symbols)
            logger.info("Default symbols inserted into ss_symbols table.")
        conn.close()
        logger.info("Database tables created successfully.")
    except SQLAlchemyError as err:
        logger.error(f"Error creating database tables: {err}")
        raise


def fetch_data_with_retries(symbol, max_retries=5):
    """
    Fetches data from the Twelve Data API for a given symbol with retry logic.
    """
    url = f'https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=400&format=JSON&timezone=exchange&previous_close=true&apikey={API_KEY}'
    retries = 0
    wait_times = [60, 300, 600, 1200]  # 1 min, 5 min, 10 min, 20 min
    while retries < max_retries:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            if 'status' in data and data['status'] == 'error':
                logger.error(f"API error for {symbol}: {data['message']}")
                return None
            return data
        except requests.exceptions.RequestException as err:
            logger.error(f"Request error for {symbol}: {err}")
            if retries < len(wait_times):
                wait_time = wait_times[retries]
            else:
                wait_time = wait_times[-1]
            logger.info(f"Waiting for {wait_time} seconds before retrying...")
            time.sleep(wait_time)
            retries += 1
    logger.error(f"Failed to fetch data for {symbol} after {max_retries} retries.")
    return None


def store_data(symbol, data):
    """
    Stores the fetched data into the MySQL database.
    If data for the same symbol and date already exists and is identical, it removes the old entry to avoid duplicates.
    """
    try:
        if data and "values" in data:
            df = pd.DataFrame(data["values"])
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.sort_values('datetime', inplace=True)
            df['symbol'] = symbol
            df['created_at'] = datetime.now()

            data_columns = ['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'previous_close', 'created_at']
            df_data = df[data_columns]

            conn = engine.connect()
            for index, row in df_data.iterrows():
                # Check if record exists
                s = select([ss_data]).where(and_(ss_data.c.symbol == row['symbol'], ss_data.c.datetime == row['datetime']))
                result = conn.execute(s).fetchone()
                if result:
                    # Compare data
                    existing_data = dict(result)
                    new_data = row.to_dict()
                    identical = True
                    for field in ['open', 'high', 'low', 'close', 'volume', 'previous_close']:
                        if float(existing_data[field]) != float(new_data[field]):
                            identical = False
                            break
                    if identical:
                        # Data is identical, remove old entries to avoid duplicates
                        logger.info(f"Identical data found for {symbol} on {row['datetime']}, removing old entries.")
                        # Remove from ss_data
                        delete_stmt = ss_data.delete().where(and_(ss_data.c.symbol == row['symbol'], ss_data.c.datetime == row['datetime']))
                        conn.execute(delete_stmt)
                        # Remove from ss_calculations
                        delete_stmt = ss_calculations.delete().where(and_(ss_calculations.c.symbol == row['symbol'], ss_calculations.c.datetime == row['datetime']))
                        conn.execute(delete_stmt)
                # Insert new data
                insert_stmt = ss_data.insert().values(**row.to_dict())
                conn.execute(insert_stmt)
            conn.close()
            logger.info(f"Data stored for {symbol}")
        else:
            logger.info(f"No data to store for {symbol}")
    except Exception as err:
        logger.error(f"Failed to store data for {symbol}: {err}")
        raise


def calculate_indicators(df):
    """
    Calculates financial indicators based on the data DataFrame.
    """
    try:
        # Ensure all columns are float type for calculations
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(int)
        df['previous_close'] = df['previous_close'].astype(float)

        # Calculate the percentage difference from previous close
        df['percentage_diff'] = ((df['close'] - df['previous_close']) / df['previous_close']) * 100

        # Simple Moving Averages
        for period in [5, 9, 10, 20, 50, 100, 200, 500]:
            df[f'sma_{period}'] = df['close'].rolling(window=period).mean()

        # Bollinger Bands
        sma_20 = df['close'].rolling(window=20).mean()
        std_20 = df['close'].rolling(window=20).std()
        df['bollinger_upper'] = sma_20 + (2 * std_20)
        df['bollinger_lower'] = sma_20 - (2 * std_20)

        # Percentage B indicator
        df['percent_b'] = (df['close'] - df['bollinger_lower']) / (df['bollinger_upper'] - df['bollinger_lower'])

        # MACD
        ema_12 = df['close'].ewm(span=12, adjust=False).mean()
        ema_26 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = ema_12 - ema_26
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()

        # RSI
        delta = df['close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        average_gain = gain.rolling(window=14).mean()
        average_loss = loss.rolling(window=14).mean()
        rs = average_gain / average_loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # EMA for 9 days
        df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()

        return df
    except Exception as err:
        logger.error(f"Failed to calculate indicators: {err}")
        raise


def store_calculations(df):
    """
    Stores the calculated indicators into the MySQL database.
    """
    try:
        df['calculated_at'] = datetime.now()
        calculation_columns = [
            'symbol', 'datetime', 'percentage_diff', 'sma_5', 'sma_9', 'sma_10', 'sma_20', 'sma_50',
            'sma_100', 'sma_200', 'sma_500', 'bollinger_upper', 'bollinger_lower', 'percent_b',
            'macd', 'macd_signal', 'rsi', 'ema_9', 'calculated_at'
        ]
        df_calculations = df[calculation_columns]
        df_calculations = df_calculations.replace({np.nan: None})

        conn = engine.connect()
        for index, row in df_calculations.iterrows():
            # Check if calculation already exists
            s = select([ss_calculations]).where(
                and_(
                    ss_calculations.c.symbol == row['symbol'],
                    ss_calculations.c.datetime == row['datetime']
                )
            )
            result = conn.execute(s).fetchone()
            if result:
                # Update existing calculation
                update_stmt = ss_calculations.update().where(
                    and_(
                        ss_calculations.c.symbol == row['symbol'],
                        ss_calculations.c.datetime == row['datetime']
                    )
                ).values(**row.to_dict())
                conn.execute(update_stmt)
            else:
                # Insert new calculation
                insert_stmt = ss_calculations.insert().values(**row.to_dict())
                conn.execute(insert_stmt)
        conn.close()
        logger.info("Calculations stored successfully.")
    except Exception as err:
        logger.error(f"Failed to store calculations: {err}")
        raise


def main():
    """
    Main function to orchestrate data fetching, storage, calculations, and storage of calculations.
    """
    errors_occurred = False
    try:
        create_database()
    except Exception as err:
        logger.error(f"Error in creating database: {err}")
        errors_occurred = True
        return

    # Get symbols from ss_symbols table
    try:
        conn = engine.connect()
        s = select([ss_symbols.c.symbol])
        result = conn.execute(s)
        symbols = [row['symbol'] for row in result]
        conn.close()
    except Exception as err:
        logger.error(f"Error fetching symbols from database: {err}")
        errors_occurred = True
        return

    new_data_added = False
    for symbol in symbols:
        data = fetch_data_with_retries(symbol)
        if data:
            try:
                store_data(symbol, data)
                # Check if new data for today has been added
                today = datetime.now().date()
                conn = engine.connect()
                s = select([ss_data.c.datetime]).where(
                    and_(
                        ss_data.c.symbol == symbol,
                        ss_data.c.datetime >= datetime.combine(today, datetime.min.time())
                    )
                )
                result = conn.execute(s).fetchone()
                conn.close()
                if result:
                    new_data_added = True
            except Exception as err:
                logger.error(f"Error storing data for {symbol}: {err}")
                errors_occurred = True
        else:
            logger.info(f"Failed to fetch data for {symbol}")

        # Sleep for 15 seconds between API calls to comply with rate limits
        logger.info(f"Sleeping for 15 seconds to comply with API rate limits.")
        time.sleep(15)

    if new_data_added:
        # Proceed to calculations
        try:
            conn = engine.connect()
            for symbol in symbols:
                # Fetch data for the symbol
                s = select([ss_data]).where(ss_data.c.symbol == symbol).order_by(ss_data.c.datetime.asc())
                df = pd.read_sql(s, conn)
                if not df.empty:
                    calculated_df = calculate_indicators(df)
                    store_calculations(calculated_df)
                else:
                    logger.info(f"No data available to calculate indicators for {symbol}.")
            conn.close()
        except Exception as err:
            logger.error(f"Error during calculations: {err}")
            errors_occurred = True
    else:
        logger.info("No new data added today. Skipping calculations.")

    if not errors_occurred:
        logger.info("SUCCESS: Script completed without errors.")
    else:
        logger.error("FAILURE: Errors occurred during script execution.")


if __name__ == "__main__":
    main()

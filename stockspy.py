"""
Filename: stockspy.py
Version: 2.9
Author: drhdev
Description: This script fetches stock data from the Twelve Data API, stores it in a MySQL database using SQLAlchemy,
calculates various financial indicators, and stores the calculations back into the database.
It now updates all existing data every time it is run, including today's data.
It also handles network errors and NaN values gracefully.
"""

import logging
import argparse
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import time
from datetime import datetime
import numpy as np
import socket  # Added import for socket
from sqlalchemy import create_engine, Column, String, Float, BigInteger, DateTime, Table, MetaData, select, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.url import URL

# Get the current directory where the script is running
base_dir = os.path.dirname(os.path.abspath(__file__))

# Change the working directory to the base directory if needed
os.chdir(base_dir)

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


def parse_args():
    parser = argparse.ArgumentParser(description='Stock data fetching and analysis script.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Show log output on console.')
    return parser.parse_args()


def setup_logging(verbose=False):
    # Set up logging (overwrite old log file)
    log_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    handlers = []

    file_handler = logging.FileHandler(LOG_FILE_PATH, mode='w')
    file_handler.setFormatter(log_formatter)
    handlers.append(file_handler)

    if verbose:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        handlers.append(console_handler)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for handler in handlers:
        logger.addHandler(handler)


def create_database():
    """
    Creates the necessary tables in the MySQL database if they do not exist.
    Inserts default symbols into 'ss_symbols' if the table was just created.
    """
    try:
        metadata.create_all(engine)
        with engine.begin() as conn:
            # Check if 'ss_symbols' table is empty
            s = select(ss_symbols)
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
                    {'symbol': 'META', 'created_at': datetime.now()},
                    {'symbol': 'NFLX', 'created_at': datetime.now()}
                ]
                conn.execute(ss_symbols.insert(), default_symbols)
                logging.info("Default symbols inserted into ss_symbols table.")
            logging.info("Database tables created successfully.")
    except SQLAlchemyError as err:
        logging.error(f"Error creating database tables: {err}")
        raise


def fetch_data_with_retries(symbol, max_retries=7):
    """
    Fetches data from the Twelve Data API for a given symbol with retry logic.
    """
    url = f'https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=5000&format=JSON&timezone=exchange&previous_close=true&apikey={API_KEY}'
    retries = 0
    wait_times = [60, 120, 300, 600, 900, 1200]  # Increased wait times
    while retries < max_retries:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            if 'status' in data and data['status'] == 'error':
                logging.error(f"API error for {symbol}: {data['message']}")
                return None
            return data
        except requests.exceptions.RequestException as err:
            if isinstance(err, requests.exceptions.ConnectionError) and isinstance(err.args[0], socket.gaierror):
                logging.error(f"Name resolution error for {symbol}: {err}")
            else:
                logging.error(f"Request error for {symbol}: {err}")
            if retries < len(wait_times):
                wait_time = wait_times[retries]
            else:
                wait_time = wait_times[-1]
            logging.info(f"Waiting for {wait_time} seconds before retrying...")
            time.sleep(wait_time)
            retries += 1
    logging.error(f"Failed to fetch data for {symbol} after {max_retries} retries.")
    return None


def store_data(symbol, data):
    """
    Stores the fetched data into the MySQL database.
    Updates existing records or inserts new ones.
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

            # Replace NaN values with None
            df_data = df_data.replace({np.nan: None, 'nan': None})

            with engine.begin() as conn:
                for index, row in df_data.iterrows():
                    # Convert row to dict and ensure all NaN are None
                    row_data = {k: (v if pd.notna(v) else None) for k, v in row.to_dict().items()}
                    # Check if record exists
                    s = select(ss_data).where(and_(ss_data.c.symbol == row_data['symbol'], ss_data.c.datetime == row_data['datetime']))
                    result = conn.execute(s).fetchone()
                    if result:
                        # Update existing record
                        update_stmt = ss_data.update().where(
                            and_(
                                ss_data.c.symbol == row_data['symbol'],
                                ss_data.c.datetime == row_data['datetime']
                            )
                        ).values(**row_data)
                        conn.execute(update_stmt)
                        logging.info(f"Updated data for {symbol} on {row_data['datetime']}.")
                    else:
                        # Insert new data
                        insert_stmt = ss_data.insert().values(**row_data)
                        conn.execute(insert_stmt)
                        logging.info(f"Inserted new data for {symbol} on {row_data['datetime']}.")
            logging.info(f"Data stored for {symbol}")
        else:
            logging.info(f"No data to store for {symbol}")
    except Exception as err:
        logging.error(f"Failed to store data for {symbol}: {err}")
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
        logging.error(f"Failed to calculate indicators: {err}")
        raise


def store_calculations(df):
    """
    Stores the calculated indicators into the MySQL database.
    Updates existing calculations or inserts new ones.
    """
    try:
        df['calculated_at'] = datetime.now()
        calculation_columns = [
            'symbol', 'datetime', 'percentage_diff', 'sma_5', 'sma_9', 'sma_10', 'sma_20', 'sma_50',
            'sma_100', 'sma_200', 'sma_500', 'bollinger_upper', 'bollinger_lower', 'percent_b',
            'macd', 'macd_signal', 'rsi', 'ema_9', 'calculated_at'
        ]
        df_calculations = df[calculation_columns]
        df_calculations = df_calculations.replace({np.nan: None, 'nan': None})

        with engine.begin() as conn:
            for index, row in df_calculations.iterrows():
                # Convert row to dict and ensure all NaN are None
                row_data = {k: (v if pd.notna(v) else None) for k, v in row.to_dict().items()}
                # Check if calculation already exists
                s = select(ss_calculations).where(
                    and_(
                        ss_calculations.c.symbol == row_data['symbol'],
                        ss_calculations.c.datetime == row_data['datetime']
                    )
                )
                result = conn.execute(s).fetchone()
                if result:
                    # Update existing calculation
                    update_stmt = ss_calculations.update().where(
                        and_(
                            ss_calculations.c.symbol == row_data['symbol'],
                            ss_calculations.c.datetime == row_data['datetime']
                        )
                    ).values(**row_data)
                    conn.execute(update_stmt)
                    logging.info(f"Updated calculation for {row_data['symbol']} on {row_data['datetime']}.")
                else:
                    # Insert new calculation
                    insert_stmt = ss_calculations.insert().values(**row_data)
                    conn.execute(insert_stmt)
                    logging.info(f"Inserted new calculation for {row_data['symbol']} on {row_data['datetime']}.")
        logging.info("Calculations stored successfully.")
    except Exception as err:
        logging.error(f"Failed to store calculations: {err}")
        raise


def main():
    """
    Main function to orchestrate data fetching, storage, calculations, and storage of calculations.
    """
    args = parse_args()
    setup_logging(args.verbose)
    errors_occurred = False
    try:
        create_database()
    except Exception as err:
        logging.error(f"Error in creating database: {err}")
        errors_occurred = True
        return

    # Get symbols from ss_symbols table
    try:
        with engine.connect() as conn:
            s = select(ss_symbols.c.symbol)
            result = conn.execute(s)
            symbols = [row.symbol for row in result]
    except Exception as err:
        logging.error(f"Error fetching symbols from database: {err}")
        errors_occurred = True
        return

    for symbol in symbols:
        # Fetch and store data
        data = fetch_data_with_retries(symbol)
        if data:
            try:
                store_data(symbol, data)
            except Exception as err:
                logging.error(f"Error storing data for {symbol}: {err}")
                errors_occurred = True
        else:
            logging.info(f"Failed to fetch data for {symbol}")

        # Sleep for 15 seconds between API calls to comply with rate limits
        logging.info(f"Sleeping for 15 seconds to comply with API rate limits.")
        time.sleep(15)

    # Proceed to calculations
    try:
        with engine.connect() as conn:
            for symbol in symbols:
                # Fetch data for the symbol
                s = select(ss_data).where(ss_data.c.symbol == symbol).order_by(ss_data.c.datetime.asc())
                df = pd.read_sql(s, conn)
                if not df.empty:
                    calculated_df = calculate_indicators(df)
                    store_calculations(calculated_df)
                else:
                    logging.info(f"No data available to calculate indicators for {symbol}.")
    except Exception as err:
        logging.error(f"Error during calculations: {err}")
        errors_occurred = True

    if not errors_occurred:
        logging.info("SUCCESS: Script completed without errors.")
    else:
        logging.error("FAILURE: Errors occurred during script execution.")


if __name__ == "__main__":
    main()

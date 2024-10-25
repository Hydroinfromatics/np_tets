# import dependencies 
import pandas as pd
from dateutil import parser
from running.get_data import fetch_data_from_api
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# start Processing
API_URL = "https://mongodb-api-hmeu.onrender.com"

def preprocess_data(date_format="%d-%b-%Y %H:%M:%S"):
    """
    Preprocess the raw data with proper type handling and null value management
    """
    try:
        data = fetch_data_from_api(API_URL)
        if not data:
            logger.error("No data received from API")
            return None

        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'], format=date_format, errors='coerce')
        
        # Remove rows with invalid timestamps
        df = df.dropna(subset=['timestamp'])
        
        # Convert numeric columns with proper handling
        numeric_columns = ['FlowInd', 'TDS', 'pH', 'Depth']
        for col in numeric_columns:
            # Convert to numeric, invalid values become NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Log null value counts
        null_counts = df[numeric_columns].isnull().sum()
        if null_counts.any():
            logger.warning(f"Null values found before cleaning: {null_counts}")
        
        # Handle null values differently for each column
        # For TDS and pH, only fill with 0 if the value is actually missing
        # (not when it's legitimately 0)
        df['TDS'] = df['TDS'].ffill().bfill().fillna(0)
        df['pH'] = df['pH'].ffill().bfill().fillna(0) # Neutral pH as fallback
        df['Depth'] = df['Depth'].ffill().bfill().fillna(0)
        df['FlowInd'] = df['FlowInd'].ffill().bfill().fillna(0)
        
        # Round numeric values appropriately
        df['TDS'] = df['TDS'].round().astype('Int64')  # Using Int64 to properly handle nullable integers
        df['Depth'] = df['Depth'].round(2)
        df['pH'] = df['pH'].round(2)
        df['FlowInd'] = df['FlowInd'].round(2)
        
        # Validate data ranges
        df.loc[df['pH'] < 0, 'pH'] = 7.0  # Reset invalid pH to neutral
        df.loc[df['pH'] > 14, 'pH'] = 7.0  # Reset invalid pH to neutral
        df.loc[df['TDS'] < 0, 'TDS'] = 0  # Reset negative TDS to 0
        df.loc[df['Depth'] < 0, 'Depth'] = 0  # Reset negative depth to 0
        df.loc[df['FlowInd'] < 0, 'FlowInd'] = 0  # Reset negative flow to 0
        
        # Log final data quality check
        final_null_counts = df[numeric_columns].isnull().sum()
        if final_null_counts.any():
            logger.warning(f"Null values remaining after cleaning: {final_null_counts}")
        else:
            logger.info("Data preprocessing completed successfully")
            
        return df
        
    except Exception as e:
        logger.error(f"Error in preprocess_data: {str(e)}")
        return None

def filter_data_hourly():
    """
    Get hourly aggregated data with proper handling of null values
    """
    try:
        df = preprocess_data()
        if df is None or df.empty:
            return None
            
        df = df[(df['TDS'] > 0) & (df['pH'] > 0)]  # Filter out invalid readings
        df.set_index('timestamp', inplace=True)
        
        # Aggregate with proper handling of null values
        hourly_data = df.resample('H').agg({
            'FlowInd': 'mean',
            'Depth': 'mean',
            'TDS': 'mean',
            'pH': 'mean'
        }).round(2)
        
        # Reset index to make timestamp a column again
        hourly_data.reset_index(inplace=True)
        
        return hourly_data
        
    except Exception as e:
        logger.error(f"Error in filter_data_hourly: {str(e)}")
        return None

# Keep other filter functions (filter_data, filter_data_daily, filter_data_weekly, filter_data_monthly)

def filter_data(from_date, to_date):
    df = preprocess_data()
    from_date = parser.parse(from_date)
    to_date = parser.parse(to_date)
    return df[(df['timestamp'].dt.date >= from_date.date()) & (df['timestamp'].dt.date <= to_date.date())]


def filter_data_daily(from_date, to_date):
    df = preprocess_data()
    from_date = parser.parse(from_date)
    to_date = parser.parse(to_date)
    df = df[(df['timestamp'].dt.date >= from_date.date()) & (df['timestamp'].dt.date <= to_date.date())]
    df.set_index('timestamp', inplace=True)
    water_data = df.resample('D').agg({'FlowInd': 'mean', 'Depth': 'mean'}).reset_index()
    df = df[(df['TDS'] != 0)]
    df = df[(df['pH'] != 0)]
    tds_ph_data = df.resample('D').agg({'TDS': 'mean', 'pH': 'mean'}).reset_index()
    return water_data.merge(tds_ph_data, how='inner', on='timestamp')


def filter_data_weekly(from_date, to_date):
    df = preprocess_data()
    from_date = parser.parse(from_date)
    to_date = parser.parse(to_date)
    df = df[(df['timestamp'].dt.date >= from_date.date()) & (df['timestamp'].dt.date <= to_date.date())]
    df.set_index('timestamp', inplace=True)
    return df.resample('W-Mon').agg({'FlowInd': 'sum', 'Depth': 'sum', 'TDS': 'mean', 'pH': 'mean'}).reset_index()


def filter_data_monthly(from_date, to_date):
    df = preprocess_data()
    from_date = parser.parse(from_date)
    to_date = parser.parse(to_date)
    df = df[(df['TDS'] != 0)]
    df = df[(df['pH'] != 0)]
    df = df[(df['timestamp'].dt.date >= from_date.date()) & (df['timestamp'].dt.date <= to_date.date())]
    df.set_index('timestamp', inplace=True)
    return df.resample('ME').agg({'FlowInd': 'sum', 'Depth': 'sum', 'TDS': 'mean', 'pH': 'mean'}).reset_index()


def filter_data_hourly():
    df = preprocess_data()
    df = df[(df['TDS'] != 0)]
    df = df[(df['pH'] != 0)]
    df.set_index('timestamp', inplace=True)
    return df.resample('h').agg({'FlowInd': 'sum', 'Depth': 'sum', 'TDS': 'mean', 'pH': 'mean'}).reset_index()
# but update them similarly if needed
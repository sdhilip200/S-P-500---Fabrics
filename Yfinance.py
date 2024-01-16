!pip install yfinance
!pip install pandas_datareader

# Importing libraries

import yfinance as yf
import pandas as pd
import datetime
import os
from pandas_datareader import data as pdr
from datetime import datetime as dt, timedelta
from pyspark.sql import SparkSession

# List of stock symbols
symbols_list = ['AAPL', 'AMZN', 'ORCL', 'TSLA', 'NVDA', 'MSFT', 'BTC-USD']

# Placeholder for the fetched data
dataframes = []

# Set the start date to 24 hours ago and the end date to the current date
# For historic data, change the number of days
start_date = (dt.today() - timedelta(days=1)).strftime('%Y-%m-%d')
end_date = dt.today().strftime('%Y-%m-%d')

# Loop through each symbol
for symbol in symbols_list:
    try:
        # Try to fetch the data using Yahoo Finance API (yfinance)
        data = yf.download(symbol, start=start_date, end=end_date)
        
        # Reset the index so that the date becomes a column
        data = data.reset_index()

        # Add a new column for the stock symbol
        data['Symbol'] = symbol
        
        # Append the data to the list if not empty
        if not data.empty:
            dataframes.append(data)
    
    except Exception as e:
        # Print an error message if data fetching fails for a symbol
        print(f"Error fetching data for {symbol}: {e}")

# Combine all the individual dataframes into one
full_data = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

# Rename the "Adj Close" column to "AdjClose"
full_data.rename(columns={"Adj Close": "AdjClose"}, inplace=True)

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Convert Pandas DataFrame to PySpark DataFrame
spark_df = spark.createDataFrame(full_data)

# Delta Lake path for storing the data
delta_path = "Tables/Stocks"

# Write the PySpark DataFrame to Delta Lake in append mode
spark_df.write.format("delta").mode("append").save(delta_path)
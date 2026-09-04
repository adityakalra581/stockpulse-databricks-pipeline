# Databricks notebook source
# MAGIC %md
# MAGIC ## Stock Market Live Data Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Creating a staging Table
# MAGIC - Notebook 1 — this is Bronze ingestion notebook
# MAGIC - Fetches from Massiv API, stores raw JSON to tbl_staging(bronze layer)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1a: Add ticker info data in staging table

# COMMAND ----------

import requests
import time
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

import json
# Provide your API key directly
# TODO: Replace with your actual API key
api_key = "3PQab2J8JDhGlwgGfEmhhhzdXB9GVuow"

# COMMAND ----------

# API configuration
api_endpoint = "https://api.massive.com/v3/reference/tickers"



# Fetch data from API
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

response = requests.get(api_endpoint, headers=headers)
response.raise_for_status()  # Raise error if request failed

# Parse JSON response

response_data = response.json()

# Extract the results array (each ticker is a separate dictionary)
results = response_data.get('results', [])

# Create one row per dictionary/object
data = []
for item in results:
    # Add the ReportType field to each item
    item['ReportType'] = 'Ticker_Info'
    # Convert each dictionary to JSON string
    raw_json = json.dumps(item)
    data.append((raw_json, api_endpoint))

schema = StructType([
    StructField("raw", StringType(), False),
    StructField("source", StringType(), False)
])

df = spark.createDataFrame(data, schema)
df = df.withColumn("loaded", current_timestamp())

# Reorder columns to match requirements: raw, loaded, source
df = df.select("raw", "loaded", "source")

# Create table if it doesn't exist and insert data
table_name = "workspace.default.tbl_staging_stock_data"

df.write.mode("append").saveAsTable(table_name)

print(f"Successfully loaded data from {api_endpoint} to {table_name}")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT raw:ticker as ticker 
# MAGIC     FROM workspace.default.tbl_staging_stock_data 
# MAGIC     WHERE raw:ReportType = 'Ticker_Info';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Previous day closing price + OHLC

# COMMAND ----------

# Get list of ticker symbols from the staging table
ticker_df = spark.sql("""
    SELECT DISTINCT raw:ticker as ticker 
    FROM workspace.default.tbl_staging_stock_data 
    WHERE raw:ReportType = 'Ticker_Info'
""")
ticker_symbols = [row.ticker for row in ticker_df.collect()]

print(f"Found {len(ticker_symbols)} tickers to fetch")
print(f"Rate limit: 5 calls/minute (12 seconds between calls)")
print(f"Estimated time: ~{len(ticker_symbols) * 12 / 60:.1f} minutes\n")

# Fetch previous day bar for each ticker
all_ticker_data = []
start_time = time.time()

for i, ticker in enumerate(ticker_symbols, 1):
    try:
        api_endpoint = f"https://api.massive.com/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={api_key}"
        
        response = requests.get(api_endpoint)
        response.raise_for_status()
        
        response_data = response.json()
        
        # Add ReportType to the response
        response_data['ReportType'] = 'previous_day_bar'
        response_data['ticker'] = ticker
        
        all_ticker_data.append({
            'raw_json': json.dumps(response_data),
            'source': api_endpoint
        })
        
        print(f"✓ [{i}/{len(ticker_symbols)}] Fetched data for {ticker}")
        
    except Exception as e:
        print(f"✗ [{i}/{len(ticker_symbols)}] Error fetching {ticker}: {e}")
        break;
    
    # Rate limiting: wait 12 seconds between calls (5 calls per minute)
    if i < len(ticker_symbols):  # Don't wait after the last call
        time.sleep(12)

elapsed_time = time.time() - start_time
print(f"\n{'='*60}")
print(f"✓ Successfully fetched {len(all_ticker_data)}/{len(ticker_symbols)} ticker records")
print(f"✗ Failed: {len(ticker_symbols) - len(all_ticker_data)}")
print(f"⏱ Total time: {elapsed_time/60:.1f} minutes")
print(f"{'='*60}\n")

if all_ticker_data:
    print(f"Sample record: {all_ticker_data[0]['raw_json'][:200]}...")

# Convert to DataFrame and insert into staging table
if all_ticker_data:
    # Prepare data as list of tuples (raw, source)
    data = [(item['raw_json'], item['source']) for item in all_ticker_data]
    
    # Create DataFrame with schema
    schema = StructType([
        StructField("raw", StringType(), False),
        StructField("source", StringType(), False)
    ])
    
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("loaded", current_timestamp())
    
    # Reorder columns to match table: raw, loaded, source
    df = df.select("raw", "loaded", "source")
    
    # Insert into staging table
    table_name = "workspace.default.tbl_staging_stock_data"
    df.write.mode("append").saveAsTable(table_name)
    
    print(f"\n✓ Successfully inserted {len(all_ticker_data)} records into {table_name}")
    display(df)
else:
    print("\n✗ No data to insert - all API requests failed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete
# MAGIC --     FROM workspace.default.tbl_staging_stock_data 
# MAGIC --     WHERE raw:ReportType = 'previous_day_bar';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Historical daily OHLC last 90 days
# MAGIC
# MAGIC **First Run — Full 90 Day Load**
# MAGIC
# MAGIC | Parameter | Value | Example (if today is Aug 30 2026) |
# MAGIC | --- | --- | --- |
# MAGIC | from | Today minus 90 days | 2026-06-01 |
# MAGIC | to | Yesterday (markets report previous day) | 2026-08-29 |
# MAGIC
# MAGIC **Every Run After That — Incremental Daily**
# MAGIC
# MAGIC You only fetch what you don't already have. From is yesterday, to is yesterday. One day of new data per run.
# MAGIC
# MAGIC | Parameter | Value | Example (running on Aug 31) |
# MAGIC | --- | --- | --- |
# MAGIC | from | Yesterday | 2026-08-30 |
# MAGIC | to | Yesterday | 2026-08-30 |

# COMMAND ----------

## this will be first run

from datetime import datetime, timedelta

ticker_df = spark.sql("""
                      select distinct raw:ticker as ticker
                      from tbl_staging_stock_data
                      where raw:ReportType = 'Ticker_Info'""")

ticker_symbols = [row.ticker for row in ticker_df.collect()]

today = datetime.today()
to_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
from_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")

all_tickers_data = []
start_time = time.time()


for i,ticker in enumerate(ticker_symbols,1):
    try:
        api_endpoint = f"https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}?adjusted=true&sort=asc&limit=120&apiKey={api_key}"

        response = requests.get(api_endpoint)
        response.raise_for_status()

        response_data = response.json()

        ## Add ReportType to the Response
        response_data['ReportType'] = 'historical_daily_bar'
        response_data['ticker'] = ticker

        all_tickers_data.append({
            'raw_json': json.dumps(response_data),
            'source': api_endpoint
        })

        print(f"✓ [{i}/{len(ticker_symbols)}] Fetched data for {ticker}")

    except Exception as e:
        print(f"✗ [{i}/{len(ticker_symbols)}] Error fetching {ticker}: {e}")
        break;
    
    # Rate limiting: wait 12 seconds between calls (5 calls per minute)
    if i < len(ticker_symbols):  # Don't wait after the last call
        time.sleep(12)

elapsed_time = time.time() - start_time
print(f"\n{'='*60}")
print(f"✓ Successfully fetched {len(all_tickers_data)}/{len(ticker_symbols)} ticker records")
print(f"✗ Failed: {len(ticker_symbols) - len(all_tickers_data)}")
print(f"⏱ Total time: {elapsed_time/60:.1f} minutes")
print(f"{'='*60}\n")

if all_tickers_data:
    print(f"Sample record: {all_tickers_data[0]['raw_json'][:200]}...")

# Convert to DataFrame and insert into staging table
if all_tickers_data:
    # Prepare data as list of tuples (raw, source)
    data = [(item['raw_json'], item['source']) for item in all_tickers_data]
    
    # Create DataFrame with schema
    schema = StructType([
        StructField("raw", StringType(), False),
        StructField("source", StringType(), False)
    ])
    
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("loaded", current_timestamp())
    
    # Reorder columns to match table: raw, loaded, source
    df = df.select("raw", "loaded", "source")
    
    # Insert into staging table
    table_name = "workspace.default.tbl_staging_stock_data"
    df.write.mode("append").saveAsTable(table_name)
    
    print(f"\n✓ Successfully inserted {len(all_tickers_data)} records into {table_name}")
    display(df)
else:
    print("\n✗ No data to insert - all API requests failed")






# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC     FROM workspace.default.tbl_staging_stock_data 
# MAGIC     WHERE raw:ReportType = 'historical_daily_bar';

# COMMAND ----------

## Daily Run

## this will be first run

from datetime import datetime, timedelta

ticker_df = spark.sql("""
                      select distinct raw:ticker as ticker
                      from tbl_staging_stock_data
                      where raw:ReportType = 'Ticker_Info'""")

ticker_symbols = [row.ticker for row in ticker_df.collect()]

today = datetime.today()
to_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
from_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")

all_tickers_data = []
start_time = time.time()


for i,ticker in enumerate(ticker_symbols,1):
    try:
        api_endpoint = f"https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}?adjusted=true&sort=asc&limit=120&apiKey={api_key}"

        response = requests.get(api_endpoint)
        response.raise_for_status()

        response_data = response.json()

        ## Add ReportType to the Response
        response_data['ReportType'] = 'historical_daily_bar'
        response_data['ticker'] = ticker

        all_tickers_data.append({
            'raw_json': json.dumps(response_data),
            'source': api_endpoint
        })

        print(f"✓ [{i}/{len(ticker_symbols)}] Fetched data for {ticker}")

    except Exception as e:
        print(f"✗ [{i}/{len(ticker_symbols)}] Error fetching {ticker}: {e}")
        break;
    
    # Rate limiting: wait 12 seconds between calls (5 calls per minute)
    if i < len(ticker_symbols):  # Don't wait after the last call
        time.sleep(12)

elapsed_time = time.time() - start_time
print(f"\n{'='*60}")
print(f"✓ Successfully fetched {len(all_tickers_data)}/{len(ticker_symbols)} ticker records")
print(f"✗ Failed: {len(ticker_symbols) - len(all_tickers_data)}")
print(f"⏱ Total time: {elapsed_time/60:.1f} minutes")
print(f"{'='*60}\n")

if all_tickers_data:
    print(f"Sample record: {all_tickers_data[0]['raw_json'][:200]}...")

# Convert to DataFrame and insert into staging table
if all_tickers_data:
    # Prepare data as list of tuples (raw, source)
    data = [(item['raw_json'], item['source']) for item in all_tickers_data]
    
    # Create DataFrame with schema
    schema = StructType([
        StructField("raw", StringType(), False),
        StructField("source", StringType(), False)
    ])
    
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("loaded", current_timestamp())
    
    # Reorder columns to match table: raw, loaded, source
    df = df.select("raw", "loaded", "source")
    
    # Insert into staging table
    table_name = "workspace.default.tbl_staging_stock_data"
    df.write.mode("append").saveAsTable(table_name)
    
    print(f"\n✓ Successfully inserted {len(all_tickers_data)} records into {table_name}")
    display(df)
else:
    print("\n✗ No data to insert - all API requests failed")





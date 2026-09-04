# Databricks notebook source
# MAGIC %sql
# MAGIC select * from tbl_staging_stock_data where raw:ReportType = 'Ticker_Info';

# COMMAND ----------

# DBTITLE 1,Create Silver Ticker Info Table
# Create silver table using SQL (colon notation works in SQL)
spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.silver_ticker_info AS
    SELECT 
        raw:ticker as ticker,
        raw:name as name,
        raw:market as market,
        raw:locale as locale,
        raw:primary_exchange as primary_exchange,
        raw:type as type,
        raw:active as active,
        raw:currency_name as currency_name,
        raw:cik as cik,
        raw:composite_figi as composite_figi,
        raw:share_class_figi as share_class_figi,
        raw:last_updated_utc as last_updated_utc,
        loaded,
        source
    FROM workspace.default.tbl_staging_stock_data
    WHERE raw:ReportType = 'Ticker_Info'
""")

# Read the newly created table
df_ticker_info = spark.table("workspace.default.silver_ticker_info")

print(f"✓ Created table: workspace.default.silver_ticker_info")
print(f"✓ Total records: {df_ticker_info.count()}")

# Display sample data
display(df_ticker_info.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl_staging_stock_data where raw:ReportType = 'historical_daily_bar';

# COMMAND ----------

# DBTITLE 1,Create Silver Historical Daily Bar Table
from pyspark.sql.functions import col, explode, to_date, from_unixtime

# Create silver table by exploding the nested results array
spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.silver_historical_daily_bar AS
    SELECT 
        raw:ticker as ticker,
        raw:queryCount as query_count,
        raw:resultsCount as results_count,
        raw:adjusted as adjusted,
        exploded_result.v as volume,
        exploded_result.vw as volume_weighted_price,
        exploded_result.o as open_price,
        exploded_result.c as close_price,
        exploded_result.h as high_price,
        exploded_result.l as low_price,
        exploded_result.t as timestamp_ms,
        to_date(from_unixtime(exploded_result.t / 1000)) as trade_date,
        exploded_result.n as num_transactions,
        exploded_result.otc as is_otc,
        loaded,
        source
    FROM workspace.default.tbl_staging_stock_data
    LATERAL VIEW explode(from_json(raw:results, 'array<struct<v:double,vw:double,o:double,c:double,h:double,l:double,t:bigint,n:int,otc:boolean>>')) AS exploded_result
    WHERE raw:ReportType = 'historical_daily_bar'
""")

# Read the newly created table
df_historical = spark.table("workspace.default.silver_historical_daily_bar")

print(f"✓ Created table: workspace.default.silver_historical_daily_bar")
print(f"✓ Total records: {df_historical.count()}")
print(f"✓ Unique tickers: {df_historical.select('ticker').distinct().count()}")

# Display sample data
display(df_historical.orderBy(col("ticker"), col("trade_date")).limit(20))

# COMMAND ----------

# DBTITLE 1,Query Previous Day Bar Data
# MAGIC %sql
# MAGIC select * from tbl_staging_stock_data where raw:ReportType = 'previous_day_bar';

# COMMAND ----------

# DBTITLE 1,Create Silver Previous Day Bar Table
from pyspark.sql.functions import col, explode, to_date, from_unixtime

# Create silver table by exploding the nested results array
# Note: Removing T field from struct to avoid case-sensitivity conflict with t (timestamp)
spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.silver_previous_day_bar AS
    SELECT 
        raw:ticker as ticker,
        raw:queryCount as query_count,
        raw:resultsCount as results_count,
        raw:adjusted as adjusted,
        raw:status as status,
        raw:request_id as request_id,
        result_data.v as volume,
        result_data.vw as volume_weighted_price,
        result_data.o as open_price,
        result_data.c as close_price,
        result_data.h as high_price,
        result_data.l as low_price,
        result_data.t as timestamp_ms,
        to_date(from_unixtime(result_data.t / 1000)) as trade_date,
        result_data.n as num_transactions,
        result_data.otc as is_otc,
        loaded,
        source
    FROM workspace.default.tbl_staging_stock_data
    LATERAL VIEW explode(from_json(raw:results, 'array<struct<v:double,vw:double,o:double,c:double,h:double,l:double,t:bigint,n:int,otc:boolean>>')) AS result_data
    WHERE raw:ReportType = 'previous_day_bar'
    AND raw:resultsCount > 0
""")

# Read the newly created table
df_previous_day = spark.table("workspace.default.silver_previous_day_bar")

print(f"✓ Created table: workspace.default.silver_previous_day_bar")
print(f"✓ Total records: {df_previous_day.count()}")
print(f"✓ Unique tickers: {df_previous_day.select('ticker').distinct().count()}")

# Display sample data
display(df_previous_day.orderBy(col("ticker")).limit(20))
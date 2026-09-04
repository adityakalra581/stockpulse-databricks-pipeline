# Databricks notebook source
# MAGIC %sql
# MAGIC select * from silver_previous_day_bar;
# MAGIC select * from silver_historical_daily_bar;
# MAGIC select * from silver_ticker_info;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE silver_ticker_info;

# COMMAND ----------

from pyspark.sql.functions import (
    col, avg, lag, round as spark_round, 
    when, rank, current_timestamp
)
from pyspark.sql import Window

print("Imports done")

# Read all three silver tables
previous_day = spark.table("silver_previous_day_bar")
historical = spark.table("silver_historical_daily_bar")
ticker_info = spark.table("silver_ticker_info")

print(f"Previous day rows: {previous_day.count()}")
print(f"Historical rows: {historical.count()}")
print(f"Ticker info rows: {ticker_info.count()}")

# Window ordered by date per ticker — for lag function
window_by_date = Window.partitionBy("ticker").orderBy("trade_date")

# Rolling 7-day window per ticker
window_7 = Window.partitionBy("ticker") \
                 .orderBy("trade_date") \
                 .rowsBetween(-6, 0)

# Rolling 30-day window per ticker
window_30 = Window.partitionBy("ticker") \
                  .orderBy("trade_date") \
                  .rowsBetween(-29, 0)

# Compute moving averages and daily return on historical data
historical_enriched = historical.select(
    "ticker",
    "trade_date",
    "close_price",
    "open_price",
    "high_price",
    "low_price",
    "volume",
    "volume_weighted_price"
) \
.withColumn("ma_7_day", 
    spark_round(avg("close_price").over(window_7), 2)) \
.withColumn("ma_30_day", 
    spark_round(avg("close_price").over(window_30), 2)) \
.withColumn("prev_close", 
    lag("close_price", 1).over(window_by_date)) \
.withColumn("daily_return_pct",
    when(col("prev_close").isNull() | (col("prev_close") == 0), None)
    .otherwise(
        spark_round(
            ((col("close_price") - col("prev_close")) / col("prev_close")) * 100
        , 2)
    )
)

print("Moving averages computed")
historical_enriched.orderBy("ticker", "trade_date").show(5)





# COMMAND ----------

# Rank rows per ticker by date descending — latest row gets rank 1
window_latest = Window.partitionBy("ticker") \
                      .orderBy(col("trade_date").desc())

latest_historical = historical_enriched \
    .withColumn("row_num", rank().over(window_latest)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

print(f"Latest historical rows (should equal ticker count): {latest_historical.count()}")
latest_historical.show(5)

# COMMAND ----------

# Select only what you need from previous day table
prev_day_clean = previous_day.select(
    "ticker",
    col("close_price").alias("current_close"),
    col("open_price").alias("current_open"),
    col("high_price").alias("current_high"),
    col("low_price").alias("current_low"),
    col("volume").alias("current_volume"),
    col("volume_weighted_price").alias("current_vwap"),
    col("num_transactions").alias("current_transactions"),
    col("trade_date").alias("latest_trade_date")
)

print(f"Previous day rows: {prev_day_clean.count()}")
prev_day_clean.show(5)

# COMMAND ----------

# Select company details from ticker_info
company_info = ticker_info.select(
    "ticker",
    col("name").alias("company_name"),
    col("market"),
    col("primary_exchange").alias("exchange"),
    col("type").alias("ticker_type"),
    col("currency_name").alias("currency"),
    col("active")
)

print(f"Company info rows: {company_info.count()}")
company_info.show(5)

# COMMAND ----------

# Step 1 — Join latest historical (has moving averages) with previous day (has current price)
gold_df = latest_historical.join(
    prev_day_clean, 
    on="ticker", 
    how="left"
)

# Step 2 — Join with company info
gold_df = gold_df.join(
    company_info,
    on="ticker",
    how="left"
)

print(f"Gold rows after joins: {gold_df.count()}")
gold_df.show(5)

# COMMAND ----------

# Add trend signals and metadata
gold_final = gold_df.select(
    # Identity
    col("ticker"),
    col("company_name"),
    col("exchange"),
    col("currency"),
    col("market"),
    
    # Current price info from previous_day_bar
    col("latest_trade_date"),
    col("current_close"),
    col("current_open"),
    col("current_high"),
    col("current_low"),
    col("current_volume"),
    col("current_vwap"),
    col("current_transactions"),
    
    # Moving averages computed from historical
    col("ma_7_day"),
    col("ma_30_day"),
    col("daily_return_pct"),
    
    # Business logic — trend signals
    when(col("current_close") > col("ma_7_day"), "ABOVE")
        .otherwise("BELOW").alias("vs_ma7_signal"),
    
    when(col("current_close") > col("ma_30_day"), "ABOVE")
        .otherwise("BELOW").alias("vs_ma30_signal"),
    
    when(col("daily_return_pct") > 0, "POSITIVE")
        .when(col("daily_return_pct") < 0, "NEGATIVE")
        .otherwise("NEUTRAL").alias("day_sentiment"),
    
    # Price range of the day as percentage
    spark_round(
        ((col("current_high") - col("current_low")) / col("current_low")) * 100
    , 2).alias("day_range_pct"),
    
    # Metadata
    current_timestamp().alias("gold_computed_at")
)

print(f"Final gold rows: {gold_final.count()}")
gold_final.show(5)

# COMMAND ----------

gold_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_stock_summary")

print("Gold table written successfully")
print(f"Total tickers in gold: {spark.table('gold_stock_summary').count()}")

# COMMAND ----------

# Final check — see what your dashboard will read
spark.sql("""
    SELECT 
        ticker,
        company_name,
        exchange,
        current_close,
        daily_return_pct,
        day_sentiment,
        ma_7_day,
        ma_30_day,
        vs_ma7_signal,
        vs_ma30_signal,
        day_range_pct,
        gold_computed_at
    FROM gold_stock_summary
    ORDER BY daily_return_pct DESC
""").show(20, truncate=False)
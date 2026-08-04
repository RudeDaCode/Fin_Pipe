from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, max, count, when

spark = (
    SparkSession.builder
    .appName("Financial Analytics")
    .getOrCreate()
)

df = spark.read.parquet("data/processed/transactions")

# ==========================================
# 1. MERCHANT SUMMARY
# ==========================================
merchant_summary = (
    df.groupBy("merchant")
      .agg(
          count("*").alias("transactions"),
          sum("amount_eur").alias("total_revenue_eur"),
          avg("amount_eur").alias("average_transaction_eur"),
          max("amount_eur").alias("largest_transaction_eur")
      )
      .orderBy("total_revenue_eur", ascending=False)
)
merchant_summary.write.mode("overwrite").parquet("data/analytics/merchant_summary")


# ==========================================
# 2. TIME SUMMARY (Replaces Hourly, Day, and Weekend)
# Grouping by day and hour yields a max of 168 rows. 
# Power BI can use this one table to slice by hour, day, or weekend status.
# ==========================================
time_summary = (
    df.groupBy("day_of_week", "is_weekend", "hour")
      .agg(
          count("*").alias("transactions"),
          sum("amount_eur").alias("total_revenue_eur"),
          avg("amount_eur").alias("average_transaction_eur")
      )
)

# Add day_number for correct chronological sorting in Power BI
time_summary = (
    time_summary.withColumn(
        "day_number",
        when(time_summary.day_of_week == "Monday", 1)
        .when(time_summary.day_of_week == "Tuesday", 2)
        .when(time_summary.day_of_week == "Wednesday", 3)
        .when(time_summary.day_of_week == "Thursday", 4)
        .when(time_summary.day_of_week == "Friday", 5)
        .when(time_summary.day_of_week == "Saturday", 6)
        .otherwise(7)
    )
    .orderBy("day_number", "hour")
)
time_summary.write.mode("overwrite").parquet("data/analytics/time_summary")


# ==========================================
# 3. CATEGORY SUMMARY (Replaces Category Spending & Category Day)
# ==========================================
category_summary = (
    df.groupBy("category")
      .agg(
          count("*").alias("transactions"),
          sum("amount_eur").alias("total_revenue_eur"),
          avg("amount_eur").alias("average_transaction_eur"),
          max("amount_eur").alias("largest_transaction_eur")
      )
      .orderBy("total_revenue_eur", ascending=False)
)
category_summary.write.mode("overwrite").parquet("data/analytics/category_summary")


# ==========================================
# 4. COUNTRY SUMMARY
# ==========================================
country_summary = (
    df.groupBy("country")
      .agg(
          count("*").alias("transactions"),
          sum("amount_eur").alias("total_revenue_eur"),
          avg("amount_eur").alias("average_transaction_eur")
      )
      .orderBy("total_revenue_eur", ascending=False)
)
country_summary.write.mode("overwrite").parquet("data/analytics/country_summary")


# ==========================================
# 5. PAYMENT SUMMARY
# ==========================================
payment_summary = (
    df.groupBy("payment_method")
      .agg(
          count("*").alias("transactions"),
          sum("amount_eur").alias("total_revenue_eur"),
          avg("amount_eur").alias("average_transaction_eur")
      )
      .orderBy("transactions", ascending=False)
)
payment_summary.write.mode("overwrite").parquet("data/analytics/payment_summary")


# ==========================================
# 6. CUSTOMER SUMMARY (Replaces Top Customers)
# ==========================================
customer_summary = (
    df.groupBy("customer_id")
      .agg(
          count("*").alias("transactions"),
          sum("amount_eur").alias("total_spent_eur"),
          avg("amount_eur").alias("average_transaction_eur"),
          max("amount_eur").alias("largest_transaction_eur")
      )
      .orderBy("total_spent_eur", ascending=False)
      .limit(1000) # Remove this limit if you want all customers
)
customer_summary.write.mode("overwrite").parquet("data/analytics/customer_summary")
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, max, min, countDistinct, count, when, dense_rank
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("Financial Analytics")
    .getOrCreate()
)

df = spark.read.parquet("data/processed/transactions")

# 1. Merchant Revenue
merchant_revenue = (
    df.groupBy("merchant")
      .agg(sum("amount_eur").alias("total_revenue_eur"))
      .orderBy("total_revenue_eur", ascending=False)
)
merchant_revenue.write.mode("overwrite").parquet("data/analytics/merchant_revenue")

# 2. Merchant Transactions
merchant_transactions = (
    df.groupBy("merchant")
      .agg(count("*").alias("transactions"))
      .orderBy("transactions", ascending=False)
)
merchant_transactions.write.mode("overwrite").parquet("data/analytics/merchant_transactions")

# 3. Merchant Average
merchant_average = (
    df.groupBy("merchant")
      .agg(avg("amount_eur").alias("average_transaction_eur"))
      .orderBy("average_transaction_eur", ascending=False)
)
merchant_average.write.mode("overwrite").parquet("data/analytics/merchant_average")

# 4. Country Spending
country_spending = (
    df.groupBy("country")
      .agg(sum("amount_eur").alias("total_spending_eur"))
      .orderBy("total_spending_eur", ascending=False)
)
country_spending.write.mode("overwrite").parquet("data/analytics/country_spending")

# 5. Category Spending
category_spending = (
    df.groupBy("category")
      .agg(sum("amount_eur").alias("total_spending_eur"))
      .orderBy("total_spending_eur", ascending=False)
)
category_spending.write.mode("overwrite").parquet("data/analytics/category_spending")

# 6. Time Series: Hourly Revenue
hourly_revenue = (
    df.groupBy("hour")
      .agg(sum("amount_eur").alias("total_revenue_eur"))
      .orderBy("hour")
)
hourly_revenue.write.mode("overwrite").parquet("data/analytics/hourly_revenue")

# 7. Time Series: Hourly Transactions
hourly_transactions = (
    df.groupBy("hour")
      .agg(count("*").alias("transactions"))
      .orderBy("hour")
)
hourly_transactions.write.mode("overwrite").parquet("data/analytics/hourly_transactions")

# 8. Time Series: Hourly Average
hourly_average = (
    df.groupBy("hour")
      .agg(avg("amount_eur").alias("average_transaction_eur"))
      .orderBy("hour")
)
hourly_average.write.mode("overwrite").parquet("data/analytics/hourly_average")

# 9. Day Revenue
day_revenue = (
    df.groupBy("day_of_week")
      .agg(sum("amount_eur").alias("total_revenue_eur"))
)

day_revenue = (
    day_revenue.withColumn(
        "day_number",
        when(df.day_of_week == "Monday", 1)
        .when(df.day_of_week == "Tuesday", 2)
        .when(df.day_of_week == "Wednesday", 3)
        .when(df.day_of_week == "Thursday", 4)
        .when(df.day_of_week == "Friday", 5)
        .when(df.day_of_week == "Saturday", 6)
        .otherwise(7)
    )
    .orderBy("day_number")
    .drop("day_number")
)
day_revenue.write.mode("overwrite").parquet("data/analytics/day_revenue")

# 10. Weekend Summary
weekend_summary = (
    df.groupBy("is_weekend")
      .agg(
          count("*").alias("transactions"),
          sum("amount_eur").alias("total_revenue_eur"),
          avg("amount_eur").alias("average_transaction_eur")
      )
)
weekend_summary.write.mode("overwrite").parquet("data/analytics/weekend_summary")

# 11. Payment Summary
payment_summary = (
    df.groupBy("payment_method")
      .agg(
          count("*").alias("transactions"),
          sum("amount_eur").alias("total_revenue_eur")
      )
      .orderBy("transactions", ascending=False)
)
payment_summary.write.mode("overwrite").parquet("data/analytics/payment_summary")

# 12. Category Day
category_day = (
    df.groupBy("day_of_week", "category")
      .agg(sum("amount_eur").alias("total_revenue_eur"))
)
category_day.write.mode("overwrite").parquet("data/analytics/category_day")

# 13. Top Customers
customer_spending = (
    df.groupBy("customer_id")
      .agg(sum("amount_eur").alias("total_spent_eur"))
)
ranking_window = Window.orderBy(customer_spending["total_spent_eur"].desc())
top_customers = (
    customer_spending
    .orderBy("total_spent_eur", ascending=False)
    .limit(1000) # Remove this limit if you want all customers
)
top_customers.write.mode("overwrite").parquet("data/analytics/top_customers")
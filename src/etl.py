from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    when,
    col,
    hour,
    dayofweek,
    date_format
)
spark = (
    SparkSession.builder
    .appName("Financial Pipeline")
    .getOrCreate()
)

df = spark.read.csv(
    "data/raw/transactions.csv",
    header=True,
    #simplify for now
    inferSchema=True
)
'''
df.printSchema()

df.show(5)

print(df.count())
'''
# figure out missing values/ issues with code
print("Missing merchants:",
      df.filter(df.merchant.isNull()).count())

print("Missing customers:",
      df.filter(df.customer_id.isNull()).count())

print("Negative amounts:",
      df.filter(df.amount < 0).count())

print("Invalid currencies:",
      df.filter(df.currency == "XYZ").count())

print("Invalid payment methods:",
      df.filter(df.payment_method == "Crypto Potato").count())

duplicates = df.count() - df.dropDuplicates().count()

print("Duplicate rows:", duplicates)

#fix the csv/errors

df = df.dropDuplicates()
print("Rows after removing duplicates:", df.count())

VALID_CURRENCIES = ["EUR", "USD", "INR"]
df = df.filter(df.currency.isin(VALID_CURRENCIES))
print("Rows after currency validation:", df.count())
df = df.filter(df.amount > 0)
print("Rows after removing negative amounts:", df.count())

df = df.withColumn(
    "merchant",
    when(col("merchant").isNull(), "Unknown")
    .otherwise(col("merchant"))
)

#feature engineering
df = df.withColumn(
    "hour",
    hour(col("timestamp"))
)
df = df.withColumn(
    "day_of_week",
    date_format(col("timestamp"), "EEEE")
)
df = df.withColumn(
    "is_weekend",
    when(
        col("day_of_week").isin("Saturday", "Sunday"),
        True
    ).otherwise(False)
)
df = df.withColumn(
    "high_value",
    when(col("amount") > 2000, True)
    .otherwise(False)
)
df = df.withColumn(
    "international",
    when(
        col("country") != "Netherlands",
        True
    ).otherwise(False)
)
df.select(
    "timestamp",
    "hour",
    "day_of_week",
    "is_weekend",
    "amount",
    "high_value",
    "country",
    "international"
).show(10, truncate=False)
print("Final row count:", df.count())
#create parquet
df.write.mode("overwrite").parquet("data/processed/transactions")
#verify parquet exists
parquet_df = spark.read.parquet(
    "data/processed/transactions"
)
parquet_df.printSchema()

print(parquet_df.count())

parquet_df.show(5)
spark.stop()
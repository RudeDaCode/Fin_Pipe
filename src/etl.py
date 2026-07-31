from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    when,
    col,
    hour,
    dayofweek,
    date_format,
    create_map,
    lit,
    round
)
from itertools import chain
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
EXCHANGE_RATES = {
    "EUR": 1.00,
    "USD": 0.86,
    "INR": 0.010
}
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
rate_map = create_map(
    *list(chain.from_iterable(
        [(lit(k), lit(v)) for k, v in EXCHANGE_RATES.items()]
    ))
)
#feature engineering
df = df.withColumn(
    "amount_eur",
    round(col("amount") * rate_map[col("currency")], 2)
)

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
    when(col("amount_eur") > 500, True)
    .otherwise(False)
)
df = df.withColumn(
    "international",
    when(
        col("country") != "Netherlands",
        True
    ).otherwise(False)
)
'''
df.select(
    "timestamp",
    "hour",
    "day_of_week",
    "is_weekend",
    "amount",
    "high_value",
    "country",
    "international"
).show(10, truncate=False)'''
df.select(
    "timestamp",
    "amount",
    "currency",
    "amount_eur",
    "high_value",
    "country"
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
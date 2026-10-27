from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, avg, min, max,sum as spark_sum
import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
spark = SparkSession.builder.appName("Data Exploration").master("local[2]").config("spark.driver.memory", "4g").getOrCreate()
parser = argparse.ArgumentParser()
parser.add_argument("--month", required=True)
args = parser.parse_args()
month = args.month
df = spark.read.parquet(f"silver/yellow_taxi_{month}/")

total_rows = df.count()
for column in df.columns:
    #null_count = df.filter(col(column).isNull()).count()
    #null_rate = (null_count / total_rows)*100 if total_rows > 0 else 0.0
    #print(f"{column} Count NULL Rate: {null_rate:.2f}%")
    df.agg(
        spark_sum(
            when(col(column).isNull(),1).otherwise(0)
        ).alias(f'{column}_null_count')
    ).show()
distinct_rows = df.distinct().count()
duplicate_rows = total_rows - distinct_rows
start_date = datetime.strptime(month, "%Y-%m")
end_date = start_date + relativedelta(months=1)
#INVALID VALUES,DATETIME
invalid_values = df.agg(
    count(when(col('trip_distance') < 0, 1)).alias('invalid_trip_distance'),
    count(when(col('passenger_count') < 0, 1)).alias('invalid_passenger_count')
)
invalid_row = invalid_values.first()

if invalid_row is not None:
    invalid_count = invalid_row['invalid_trip_distance']
    invalid_passenger = invalid_row['invalid_passenger_count']
else:
    invalid_count = 0
    invalid_passenger = 0
if total_rows > 0:
    invalid_rate = (invalid_count / total_rows) * 100
    invalid_passenger_rate = invalid_passenger / total_rows * 100
else:
    invalid_rate = 0
    invalid_passenger_rate = 0
#ANOMALYS
anomaly_values = df.agg(
    count(when(col('trip_distance') == 0, 1)).alias('anomaly_trip_distance'),
    count(when(col('fare_amount') <= 0, 1)).alias('anomaly_fare_amount'),

    count(when(col('total_amount') <= 0, 1)).alias('anomaly_total_amount'),
    count(when(col('passenger_count') == 0, 1)).alias('anomaly_passenger_count')
)
invalid_datetime = df.filter(col('tpep_dropoff_datetime') < col('tpep_pickup_datetime')).count()
anomaly_datetime = df.filter(col('tpep_dropoff_datetime') == col('tpep_pickup_datetime')).count()
result = df.agg(
    min('tpep_pickup_datetime').alias('earliest_pickup'),
    max('tpep_pickup_datetime').alias('latest_pickup')
).first()
outside_month = df.filter(
    ~((col('tpep_pickup_datetime')>=start_date) & (col('tpep_pickup_datetime')<end_date))
).count()
#NULL VALUES
null = df.agg(
    *[
        count(when(col(c).isNull(),1)).alias(c)
        for c in df.columns
    ]
)
null_row = null.first()
if null_row is not None:
    for c in df.columns:
        print(f"{c}: {null_row[c]}")
print("Invalid Values:")
invalid_values.show()
print(f"Invalid Datetime Count: {invalid_datetime}")
if result is not None:
    print(f"Earliest Pickup Datetime: {result['earliest_pickup']}")
    print(f"Latest Pickup Datetime: {result['latest_pickup']}")
print(f"Rows Outside {month}: {outside_month:,}")
print("\n========== DATA QUALITY REPORT ==========")

print(f"Month: {month}")
print(f"Total rows: {total_rows:,}")
print(f"Duplicate rows: {duplicate_rows:,}")

print("\n---------- INVALID VALUES ----------")

print(f"Invalid Distance Count: {invalid_count:,}")
print(f"Invalid Distance Rate: {invalid_rate:.2f}%")

print(f"Invalid Passenger Count: {invalid_passenger:,}")
print(f"Invalid Passenger Rate: {invalid_passenger_rate:.2f}%")

print("\n---------- INVALID DATETIME ----------")

print(f"Invalid Datetime Count: {invalid_datetime:,}")

print("\n=========================================")
print("\n---------- ANOMALY VALUES ----------")
anomaly_values.show()
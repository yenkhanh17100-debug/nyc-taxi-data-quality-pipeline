from pyspark.sql import SparkSession
from datetime import datetime
from src.clean import validate_silver
spark = (
    SparkSession.builder
    .appName("Test Silver Validation")
    .master("local[*]")
    .getOrCreate()
)

# Tạo 1000 dòng, trong đó có 2 dòng âm
data_two_invalid = [
    (
        datetime(2025, 1, 15, 10, 0),
        datetime(2025, 1, 15, 10, 20),
        -2.0 if i < 1 else 5.0
    )
    for i in range(1000)
]

columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance"
]

df_test = spark.createDataFrame(data_two_invalid, columns)

# Kiểm tra
df_test.show(5)

invalid_distance_count = df_test.filter(
    df_test.trip_distance < 0
).count()

invalid_distance_percentage = (
    invalid_distance_count / df_test.count()
) * 100
validate_silver(
    df_check=df_test,
    start_date=datetime(2025, 1, 1),
    next_month_date=datetime(2025, 2, 1),
    expected_rows=1000
)
print(f"Total rows: {df_test.count()}")
print(f"Invalid rows: {invalid_distance_count}")
print(f"Invalid percentage: {invalid_distance_percentage}%")
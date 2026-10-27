
import logging
import sys
import argparse
import os
from pathlib import Path
import logging
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, min, max

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# =========================================
# VALIDATE SILVER
# =========================================
def validate_silver(df_check, start_date, next_month_date, expected_rows=None):

    silver_read_count = df_check.count()
    if silver_read_count == 0:
        logging.error("Silver dataset is empty")
        raise ValueError("Silver validation failed: empty dataset")

    if expected_rows is not None:
        if silver_read_count != expected_rows:
            logging.error(
                f"Silver row count mismatch: "
                f"expected={expected_rows}, actual={silver_read_count}"
            )
            raise ValueError("Silver row count validation failed")

    outside_month = df_check.filter(
        col("tpep_pickup_datetime").isNull() |
        ~(
            (col("tpep_pickup_datetime") >= start_date) &
            (col("tpep_pickup_datetime") < next_month_date)
        )
    ).count()

    if outside_month > 0:
        logging.error(
            f"Silver month validation failed: {outside_month} invalid rows"
        )
        raise ValueError("Silver month validation failed")

    logging.info(f"Silver rows validated: {silver_read_count}")
    logging.info("Silver month validation passed")

    invalid_dropoff = df_check.filter(
    col("tpep_dropoff_datetime").isNull() |
    (
        col("tpep_dropoff_datetime") <
        col("tpep_pickup_datetime")
    )
    ).count()

    if invalid_dropoff > 0:
        logging.error(
        f"Found {invalid_dropoff} invalid dropoff datetime rows"
    )
        raise ValueError("Silver dropoff datetime validation failed")

    logging.info("Silver dropoff datetime validation passed")

    invalid_distance_count = df_check.filter(
    col("trip_distance") < 0
    ).count()
    invalid_distance_percentage = (
    invalid_distance_count / silver_read_count
    ) * 100
    if invalid_distance_percentage > 0.1:
        logging.error(
            f"Invalid trip_distance rate: "
            f"{invalid_distance_percentage:.4f}%"
        )
        raise ValueError("Silver distance validation failed")
    else:
        logging.info(
            f"PASS: Invalid distance rate = "
            f"{invalid_distance_percentage:.4f}%"
        )

# =========================================
# MAIN PIPELINE
# =========================================
def main():

    logging.info("Pipeline started")

    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True)
    args = parser.parse_args()

    month = args.month

    raw_path = f"raw/yellow_tripdata_{month}.parquet"
    silver_path = f"silver/yellow_taxi_{month}"
    staging = f'staging/yellow_taxi_{month}'
    current_month = datetime.strptime(month, "%Y-%m")
    next_month = current_month + relativedelta(months=1)

    start_date = current_month.strftime("%Y-%m-%d")
    next_month_date = next_month.strftime("%Y-%m-%d")

    spark = (
        SparkSession.builder
        .appName("Data Cleaning")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    silver_dir = Path(silver_path)
    success_file = silver_dir / "_SUCCESS"
    try:

        # =====================================
        # SILVER ALREADY EXISTS
        # =====================================
        if not silver_dir.exists():
            logging.error(f"Silver not found, processing month: {silver_dir}")

        if silver_dir.exists():
            if not success_file.is_file():
                logging.error(f'Silver success not found: {success_file}')
                raise RuntimeError( f"Incomplete Silver: missing _SUCCESS at {success_file}")
            df_check = spark.read.parquet(silver_path)
            validate_silver(
                df_check,
                start_date,
                next_month_date
            )
            logging.info(f"Skipping {month}: Silver validated")
            return

        # =====================================
        # CHECK RAW DATA
        # =====================================
        if not os.path.exists(raw_path):
            logging.error(f"Raw file not found: {raw_path}")
            sys.exit(3)

        # =====================================
        # READ RAW DATA
        # =====================================
        df = spark.read.parquet(raw_path)

        df_zones = spark.read.csv(
            "raw/taxi_zone_lookup.csv",
            header=True,
            inferSchema=True
        )

        total_before_cleaning = df.count()
        logging.info(f"Raw rows: {total_before_cleaning}")
        if total_before_cleaning == 0:
            logging.error(f"Raw dataset is empty: {raw_path}")
            raise ValueError("Raw dataset is empty")

        # =====================================
        # FILTER BY MONTH
        # =====================================
        df_clean = df.filter(
            (col("tpep_pickup_datetime") >= start_date) &
            (col("tpep_pickup_datetime") < next_month_date)
        )

        total_after_cleaning = df_clean.count()
        removed_rows = total_before_cleaning - total_after_cleaning

        logging.info(f"Outside {month} removed: {removed_rows}")

        # =====================================
        # DATETIME CLEANING
        # =====================================
        rows_before_datetime_cleaning = total_after_cleaning

        df_clean = df_clean.filter(
            col("tpep_dropoff_datetime") >=
            col("tpep_pickup_datetime")
        )

        rows_after_datetime_cleaning = df_clean.count()

        datetime_removed = (
            rows_before_datetime_cleaning -
            rows_after_datetime_cleaning
        )

        logging.info(f"Invalid datetime removed: {datetime_removed}")

        # =====================================
        # DATA QUALITY FLAGS
        # =====================================
        df_clean = df_clean.withColumn(
            "is_zero_distance_duration",
            when(
                (col("trip_distance") == 0) &
                (
                    col("tpep_dropoff_datetime") ==
                    col("tpep_pickup_datetime")
                ),
                lit(1)
            ).otherwise(lit(0))
        )

        df_clean = df_clean.withColumn(
            "is_negative_fare",
            when(
                col("fare_amount") < 0,
                lit(1)
            ).otherwise(lit(0))
        )

        df_clean = df_clean.withColumn(
            "is_disputed_negative",
            when(
                (col("total_amount") < 0) &
                (col("payment_type") == 4),
                lit(1)
            ).otherwise(lit(0))
        )

        df_clean = df_clean.withColumn(
            "is_negative_total",
            when(
                col("total_amount") < 0,
                lit(1)
            ).otherwise(lit(0))
        )

        # =====================================
        # CLEANING METRICS
        # =====================================
        final_rows = df_clean.count()
        total_removed = total_before_cleaning - final_rows

        logging.info(f"Total rows removed: {total_removed}")

        if total_before_cleaning > 0:
            removed_percentage = (
                total_removed / total_before_cleaning
            ) * 100
        else:
            removed_percentage = 0

        logging.info(f"Removed percentage: {removed_percentage:.2f}%")

        # =====================================
        # LOCATION DATA QUALITY
        # =====================================
        location_quality = df_clean.agg(
            count(
                when(col("PULocationID").isNull(), 1)
            ).alias("null_PULocationID"),

            count(
                when(col("DOLocationID").isNull(), 1)
            ).alias("null_DOLocationID"),

            count(
                when(col("PULocationID") <= 0, 1)
            ).alias("invalid_PULocationID"),

            count(
                when(col("DOLocationID") <= 0, 1)
            ).alias("invalid_DOLocationID")
        )

        location_range = df_clean.agg(
            min("PULocationID").alias("min_PULocationID"),
            max("PULocationID").alias("max_PULocationID"),
            min("DOLocationID").alias("min_DOLocationID"),
            max("DOLocationID").alias("max_DOLocationID")
        )

        orphan_pu_count = df_clean.join(
            df_zones,
            df_clean.PULocationID == df_zones.LocationID,
            "left_anti"
        ).count()

        orphan_do_count = df_clean.join(
            df_zones,
            df_clean.DOLocationID == df_zones.LocationID,
            "left_anti"
        ).count()

        # =====================================
        # ANOMALY COUNTS
        # =====================================
        negative_total_count = df_clean.filter(
            col("is_negative_total") == 1
        ).count()

        disputed_negative_count = df_clean.filter(
            col("is_disputed_negative") == 1
        ).count()

        zero_distance_duration_count = df_clean.filter(
            col("is_zero_distance_duration") == 1
        ).count()

        negative_fare = df_clean.filter(
            col("is_negative_fare") == 1
        ).count()

        logging.info(f"Negative fare detected: {negative_fare}")
        logging.info(f"Negative total detected: {negative_total_count}")
        logging.info(f"Disputed negative detected: {disputed_negative_count}")
        logging.info(
            f"Zero distance + duration detected: "
            f"{zero_distance_duration_count}"
        )

        # =====================================
        # DATA QUALITY REPORT
        # =====================================
        print("\n========== DATA QUALITY REPORT ==========")

        print("\nINPUT")
        print(f"Raw rows: {total_before_cleaning:,}")

        print("\nREMOVED")
        print(f"Outside {month}: {removed_rows:,}")
        print(f"Invalid datetime: {datetime_removed:,}")
        print(f"Total removed: {total_removed:,}")
        print(f"Removed percentage: {removed_percentage:.2f}%")

        print("\nANOMALIES KEPT")
        print(f"Negative fare: {negative_fare:,}")
        print(f"Negative total: {negative_total_count:,}")
        print(f"Disputed negative: {disputed_negative_count:,}")
        print(f"Zero distance + duration: {zero_distance_duration_count:,}")

        print("\nLOCATION QUALITY")
        location_quality.show(truncate=False)
        location_range.show(truncate=False)
        print(f"Orphan PU locations: {orphan_pu_count:,}")
        print(f"Orphan DO locations: {orphan_do_count:,}")

        print("\nOUTPUT")
        print(f"Silver rows: {final_rows:,}")
        print("=========================================\n")

        # =====================================
        # WRITE SILVER
        # =====================================
        try:
            df_clean.write.mode("overwrite").parquet(staging)
            logging.info("Staging data written successfully")

        except Exception:
            logging.exception("Failed to write Staging data")
            raise

        # =====================================
        # READ BACK AND VALIDATE SILVER
        # =====================================
        df_check = spark.read.parquet(staging)

        validate_silver(
            df_check,
            start_date,
            next_month_date,
            expected_rows=final_rows
        )
        # =====================================
        # PUBLISH STAGING TO SILVER
        # =====================================

        staging_path = Path(staging)
        silver_destination = Path(silver_path)

        if silver_destination.exists():
            raise FileExistsError(
                f"Silver already exists: {silver_destination}"
            )

        if not staging_path.is_dir():
            raise FileNotFoundError(
                f"Staging directory not found: {staging_path}"
            )

        staging_path.rename(silver_destination)

        logging.info(
            f"Successfully published Silver: {silver_destination}"
        )
        logging.info("Pipeline completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
from pyspark.sql import SparkSession, Row, types as T
import sys, traceback

PROJECT_ID = "jads-de-assignment-2"
DATASET = "de_flights"
BQ_TEMP_BUCKET = "airline-temp-data"
BQ_TABLE_TEST = f"{PROJECT_ID}.{DATASET}.daily_delay_stats_test"
BUCKET_NAME = "airline-delays-bucket"


def make_spark():
    spark = (
        SparkSession.builder
        .appName("BQConnectorTest")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "2")
        .config("spark.dynamicAllocation.enabled", "false")
        # loosen network/heartbeat timeouts to avoid premature driver shutdown
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.driver.maxResultSize", "512m")
        .getOrCreate()
    )
    return spark


def main():
    spark = make_spark()

    schema = T.StructType([
        T.StructField("FL_DATE", T.DateType(), True),
        T.StructField("AIRLINE", T.StringType(), True),
        T.StructField("AIRLINE_NAME", T.StringType(), True),
        T.StructField("ORIGIN_AIRPORT", T.StringType(), True),
        T.StructField("DESTINATION_AIRPORT", T.StringType(), True),
        T.StructField("num_flights", T.IntegerType(), True),
        T.StructField("avg_dep_delay", T.DoubleType(), True),
    ])

    # single-row sample
    rows = [
        (None, "XX", "Test Airline", "AAA", "BBB", 1, 0.0),
    ]

    df = spark.createDataFrame(rows, schema=schema)

    try:
        print(f"[INFO] Writing test table to {BQ_TABLE_TEST} via temp bucket {BQ_TEMP_BUCKET}")
        (
            df.write
            .format("bigquery")
            .option("table", BQ_TABLE_TEST)
            .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
            .mode("overwrite")
            .save()
        )
        print("[INFO] BigQuery test write succeeded")
    except Exception as e:
        tb = traceback.format_exc()
        print("[ERROR] BigQuery test write failed", file=sys.stderr)
        print(tb, file=sys.stderr)
        # attempt to log to GCS for persistence
        try:
            log_path = f"gs://{BUCKET_NAME}/logs/bq_test_error.log"
            spark.sparkContext.parallelize([tb]).saveAsTextFile(log_path)
            print(f"[INFO] Error log written to {log_path}")
        except Exception:
            print("[WARN] Could not write error log to GCS", file=sys.stderr)
        sys.exit(1)

    spark.stop()


if __name__ == '__main__':
    main()

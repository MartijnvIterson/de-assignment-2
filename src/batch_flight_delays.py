#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession, functions as F, types as T

# --------------------------------------------------------
# Hardcoded config voor jouw project / bucket / BQ
# --------------------------------------------------------
PROJECT_ID = "jads-de-assignment-2"
BUCKET_NAME = "airline-delays-bucket"   # zonder gs://
DATASET = "de_flights"

FLIGHTS_INPUT = f"gs://{BUCKET_NAME}/raw/flights/"
AIRLINES_INPUT = f"gs://{BUCKET_NAME}/raw/airlines/airlines.csv"
AIRPORTS_INPUT = f"gs://{BUCKET_NAME}/raw/airports/airports.csv"

SILVER_OUTPUT = f"gs://{BUCKET_NAME}/silver/flights_clean/"
GOLD_OUTPUT = f"gs://{BUCKET_NAME}/gold/delay_stats/"

BQ_TABLE = f"{PROJECT_ID}.{DATASET}.delay_stats" 
BQ_TEMP_BUCKET = "airline-temp-data"    

def safe_write_bigquery(spark, df, table, temp_bucket, bucket_name):
    import traceback
    try:
        (
            df.write
            .format("bigquery")
            .option("table", table)
            .option("temporaryGcsBucket", temp_bucket)
            .mode("append")
            .save()
        )
        print(f"[INFO] BigQuery write succeeded → {table}")
    except Exception as e:
        error_text = traceback.format_exc()

        # Log in Dataproc driver logs
        print("===============================================", file=sys.stderr)
        print("[ERROR] BigQuery write failed!", file=sys.stderr)
        print(f"[ERROR] Table: {table}", file=sys.stderr)
        print(f"[ERROR] Temp bucket: {temp_bucket}", file=sys.stderr)
        print("--------------- Traceback ---------------------", file=sys.stderr)
        print(error_text, file=sys.stderr)
        print("===============================================", file=sys.stderr)

        # Also log to GCS for easy debugging
        try:
            log_path = f"gs://{bucket_name}/logs/bigquery_error_{table.replace('.', '_')}.log"
            spark.sparkContext.parallelize([error_text]).saveAsTextFile(log_path)
            print(f"[INFO] Error log written to {log_path}")
        except Exception as e2:
            print("[WARN] Could not write error log to GCS.", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)


def get_flights_schema():
    return T.StructType([
        T.StructField("YEAR", T.IntegerType(), True),
        T.StructField("MONTH", T.IntegerType(), True),
        T.StructField("DAY", T.IntegerType(), True),
        T.StructField("DAY_OF_WEEK", T.IntegerType(), True),
        T.StructField("AIRLINE", T.StringType(), True),
        T.StructField("FLIGHT_NUMBER", T.StringType(), True),
        T.StructField("TAIL_NUMBER", T.StringType(), True),
        T.StructField("ORIGIN_AIRPORT", T.StringType(), True),
        T.StructField("DESTINATION_AIRPORT", T.StringType(), True),
        T.StructField("SCHEDULED_DEPARTURE", T.IntegerType(), True),
        T.StructField("DEPARTURE_TIME", T.IntegerType(), True),
        T.StructField("DEPARTURE_DELAY", T.DoubleType(), True),
        T.StructField("TAXI_OUT", T.DoubleType(), True),
        T.StructField("WHEELS_OFF", T.IntegerType(), True),
        T.StructField("SCHEDULED_TIME", T.DoubleType(), True),
        T.StructField("ELAPSED_TIME", T.DoubleType(), True),
        T.StructField("AIR_TIME", T.DoubleType(), True),
        T.StructField("DISTANCE", T.DoubleType(), True),
        T.StructField("WHEELS_ON", T.IntegerType(), True),
        T.StructField("TAXI_IN", T.DoubleType(), True),
        T.StructField("SCHEDULED_ARRIVAL", T.IntegerType(), True),
        T.StructField("ARRIVAL_TIME", T.IntegerType(), True),
        T.StructField("ARRIVAL_DELAY", T.DoubleType(), True),
        T.StructField("DIVERTED", T.IntegerType(), True),
        T.StructField("CANCELLED", T.IntegerType(), True),
        T.StructField("CANCELLATION_REASON", T.StringType(), True),
        T.StructField("AIR_SYSTEM_DELAY", T.DoubleType(), True),
        T.StructField("SECURITY_DELAY", T.DoubleType(), True),
        T.StructField("AIRLINE_DELAY", T.DoubleType(), True),
        T.StructField("LATE_AIRCRAFT_DELAY", T.DoubleType(), True),
        T.StructField("WEATHER_DELAY", T.DoubleType(), True),
    ])


def main():
    spark = (
        SparkSession.builder
        .appName("BatchFlightDelays")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "2")
        .config("spark.dynamicAllocation.enabled", "false")
        # Reduce shuffle parallelism to reduce number of tasks and memory pressure
        .config("spark.sql.shuffle.partitions", "50")
        # Prevent driver OOM when accidentally collecting large results
        .config("spark.driver.maxResultSize", "512m")
        # Reserve extra native memory for executors to avoid unexpected OOMs
        .config("spark.executor.memoryOverhead", "512")
        .getOrCreate()
    )

    flights_schema = get_flights_schema()

    flights_raw = (
        spark.read
        .option("header", "true")
        .schema(flights_schema)
        .csv(FLIGHTS_INPUT)
    )

    # Normalize airport and airline codes in the flights dataset so joins are robust
    flights_raw = (
        flights_raw
        .withColumn("ORIGIN_AIRPORT", F.trim(F.upper(F.col("ORIGIN_AIRPORT"))))
        .withColumn("DESTINATION_AIRPORT", F.trim(F.upper(F.col("DESTINATION_AIRPORT"))))
        .withColumn("AIRLINE", F.trim(F.upper(F.col("AIRLINE"))))
    )

    airlines = (
        spark.read
        .option("header", "true")
        .csv(AIRLINES_INPUT)
        .withColumnRenamed("IATA_CODE", "AIRLINE_CODE")
        .withColumnRenamed("AIRLINE", "AIRLINE_NAME")
    )

    airports = (
        spark.read
        .option("header", "true")
        .csv(AIRPORTS_INPUT)
        .withColumnRenamed("IATA_CODE", "AIRPORT_CODE")
        .withColumnRenamed("AIRPORT", "AIRPORT_NAME")
        .withColumnRenamed("CITY", "CITY_NAME")
        .withColumnRenamed("STATE", "STATE_CODE")
    )

    # Normalize airport codes and text columns so joins match (trim + upper)
    airports = (
        airports
        .withColumn("AIRPORT_CODE", F.trim(F.upper(F.col("AIRPORT_CODE"))))
        .withColumn("AIRPORT_NAME", F.trim(F.col("AIRPORT_NAME")))
        .withColumn("CITY_NAME", F.trim(F.col("CITY_NAME")))
        .withColumn("STATE_CODE", F.trim(F.col("STATE_CODE")))
    )

    # Data quality / cleaning
    flights_clean = (
        flights_raw
        .filter(F.col("CANCELLED") == 0)
        .filter(F.col("DIVERTED") == 0)
        .filter(
            F.col("AIRLINE").isNotNull()
            & F.col("ORIGIN_AIRPORT").isNotNull()
            & F.col("DESTINATION_AIRPORT").isNotNull()
            & F.col("DEPARTURE_DELAY").isNotNull()
        )
        .filter(F.col("DEPARTURE_DELAY") > -60)
        .filter(F.col("DEPARTURE_DELAY") < 1000)
        .dropDuplicates([
            "YEAR", "MONTH", "DAY",
            "FLIGHT_NUMBER", "TAIL_NUMBER",
            "ORIGIN_AIRPORT", "SCHEDULED_DEPARTURE"
        ])
    )

    # (diagnostic moved below after flights_enriched is created)

    flights_enriched = (
        flights_clean
        .join(
            airlines,
            flights_clean["AIRLINE"] == airlines["AIRLINE_CODE"],
            how="left"
        )
        .join(
            airports.select(
                F.col("AIRPORT_CODE").alias("ORIGIN_AIRPORT_CODE"),
                F.col("AIRPORT_NAME").alias("ORIGIN_AIRPORT_NAME"),
                F.col("CITY_NAME").alias("ORIGIN_CITY"),
                F.col("STATE_CODE").alias("ORIGIN_STATE")
            ),
            flights_clean["ORIGIN_AIRPORT"] == F.col("ORIGIN_AIRPORT_CODE"),
            how="left"
        )
        .join(
            airports.select(
                F.col("AIRPORT_CODE").alias("DEST_AIRPORT_CODE"),
                F.col("AIRPORT_NAME").alias("DEST_AIRPORT_NAME"),
                F.col("CITY_NAME").alias("DEST_CITY"),
                F.col("STATE_CODE").alias("DEST_STATE")
            ),
            flights_clean["DESTINATION_AIRPORT"] == F.col("DEST_AIRPORT_CODE"),
            how="left"
        )
    )

    flights_enriched = (
        flights_enriched
        .withColumn(
            "FL_DATE",
            F.to_date(
                F.concat_ws(
                    "-",
                    F.col("YEAR").cast("string"),
                    F.col("MONTH").cast("string"),
                    F.col("DAY").cast("string"),
                )
            )
        )
        .withColumn(
            "DEP_HOUR",
            (F.col("SCHEDULED_DEPARTURE") / 100).cast("int")
        )
    )

    # Diagnostic: log how many rows did not get airport enrichment and sample unmatched codes
    try:
        total_rows = flights_enriched.count()
        origin_missing = flights_enriched.filter(F.col("ORIGIN_AIRPORT_NAME").isNull()).count()
        dest_missing = flights_enriched.filter(F.col("DEST_AIRPORT_NAME").isNull()).count()

        origin_examples = [r[0] for r in flights_enriched
                           .filter(F.col("ORIGIN_AIRPORT_NAME").isNull())
                           .select("ORIGIN_AIRPORT")
                           .distinct()
                           .limit(50)
                           .collect()]

        dest_examples = [r[0] for r in flights_enriched
                         .filter(F.col("DEST_AIRPORT_NAME").isNull())
                         .select("DESTINATION_AIRPORT")
                         .distinct()
                         .limit(50)
                         .collect()]

        diag_lines = [
            f"total_rows={total_rows}",
            f"origin_missing={origin_missing}",
            f"dest_missing={dest_missing}",
            "origin_missing_examples:" ] + [str(x) for x in origin_examples] + ["dest_missing_examples:"] + [str(x) for x in dest_examples]

        diag_path = f"gs://{BUCKET_NAME}/logs/enrichment_diag_{int(__import__('time').time())}.log"
        spark.sparkContext.parallelize(diag_lines, 1).saveAsTextFile(diag_path)
        print(f"[INFO] Enrichment diagnostic written to {diag_path}")
    except Exception:
        # Non-fatal: just print to stderr if diagnostics fail
        import traceback
        print("[WARN] Could not write enrichment diagnostic:", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)

    # Silver write - make writes tolerant: limit repartition size to avoid huge shuffles
    try:
        (
            flights_enriched
            .repartition(4, "FL_DATE")
            .write
            .mode("overwrite")
            .partitionBy("FL_DATE")
            .parquet(SILVER_OUTPUT)
        )
    except Exception:
        import traceback
        err = traceback.format_exc()
        print("[ERROR] Silver parquet write failed", file=sys.stderr)
        print(err, file=sys.stderr)
        try:
            log_path = f"gs://{BUCKET_NAME}/logs/silver_parquet_error_{int(__import__('time').time())}.log"
            spark.sparkContext.parallelize([err]).saveAsTextFile(log_path)
            print(f"[INFO] Silver write error logged to {log_path}")
        except Exception:
            print("[WARN] Could not write silver error log to GCS", file=sys.stderr)

    # Add business logic columns for dashboarding
    flights_enriched = (
        flights_enriched
        .withColumn(
            "is_delayed",
            F.when(F.col("DEPARTURE_DELAY") > 15, 1).otherwise(0)
        )
        .withColumn(
            "is_on_time",
            F.when(F.col("DEPARTURE_DELAY") <= 15, 1).otherwise(0)
        )
        .withColumn(
            "delay_category",
            F.when(F.col("DEPARTURE_DELAY") <= 0, "Early/On-time")
            .when(F.col("DEPARTURE_DELAY") <= 15, "Minor")
            .when(F.col("DEPARTURE_DELAY") <= 60, "Moderate")
            .when(F.col("DEPARTURE_DELAY") <= 180, "Severe")
            .otherwise("Extreme")
        )
        .withColumn(
            "is_weekend",
            F.when(F.col("DAY_OF_WEEK").isin(6, 7), 1).otherwise(0)
        )
        .withColumn(
            "is_rush_hour",
            F.when(
                F.col("DEP_HOUR").between(6, 9) | F.col("DEP_HOUR").between(16, 19),
                1
            ).otherwise(0)
        )
        .withColumn(
            "time_of_day",
            F.when(F.col("DEP_HOUR").between(5, 11), "Morning")
            .when(F.col("DEP_HOUR").between(12, 16), "Afternoon")
            .when(F.col("DEP_HOUR").between(17, 20), "Evening")
            .otherwise("Night")
        )
        .withColumn(
            "season",
            F.when(F.col("MONTH").isin(12, 1, 2), "Winter")
            .when(F.col("MONTH").isin(3, 4, 5), "Spring")
            .when(F.col("MONTH").isin(6, 7, 8), "Summer")
            .otherwise("Fall")
        )
    )

    # Gold aggregation with unified schema for batch and stream
    agg = (
        flights_enriched
        .groupBy(
            "FL_DATE",
            "AIRLINE",
            "AIRLINE_NAME",
            "ORIGIN_AIRPORT",
            "ORIGIN_AIRPORT_NAME",
            "ORIGIN_CITY",
            "ORIGIN_STATE"
        )
        .agg(
            # Core metrics
            F.count("*").alias("num_flights"),
            F.avg("DEPARTURE_DELAY").alias("avg_dep_delay"),
            F.min("DEPARTURE_DELAY").alias("min_dep_delay"),
            F.max("DEPARTURE_DELAY").alias("max_dep_delay"),
            F.expr("percentile_approx(DEPARTURE_DELAY, 0.5)").alias("median_dep_delay"),
            F.expr("percentile_approx(DEPARTURE_DELAY, 0.9)").alias("p90_dep_delay"),
            F.expr("percentile_approx(DEPARTURE_DELAY, 0.95)").alias("p95_dep_delay"),
            
            # Delay counts and rates
            F.sum("is_delayed").alias("num_delayed"),
            F.sum("is_on_time").alias("num_on_time"),
            F.avg("is_delayed").alias("delay_rate"),
            F.avg("is_on_time").alias("on_time_rate"),
            
            # Delay severity breakdown
            F.sum(F.when(F.col("delay_category") == "Early/On-time", 1).otherwise(0)).alias("num_early_ontime"),
            F.sum(F.when(F.col("delay_category") == "Minor", 1).otherwise(0)).alias("num_minor_delay"),
            F.sum(F.when(F.col("delay_category") == "Moderate", 1).otherwise(0)).alias("num_moderate_delay"),
            F.sum(F.when(F.col("delay_category") == "Severe", 1).otherwise(0)).alias("num_severe_delay"),
            F.sum(F.when(F.col("delay_category") == "Extreme", 1).otherwise(0)).alias("num_extreme_delay"),
            
            # Time patterns
            F.sum("is_weekend").alias("num_weekend_flights"),
            F.sum("is_rush_hour").alias("num_rush_hour_flights"),
            F.avg(F.when(F.col("is_rush_hour") == 1, F.col("DEPARTURE_DELAY"))).alias("avg_delay_rush_hour"),
            F.avg(F.when(F.col("is_weekend") == 1, F.col("DEPARTURE_DELAY"))).alias("avg_delay_weekend"),
            
            # Operational metrics
            F.avg("DISTANCE").alias("avg_distance"),
            F.avg("AIR_TIME").alias("avg_air_time"),
            F.sum("CANCELLED").alias("num_cancelled"),
            F.sum("DIVERTED").alias("num_diverted")
        )
        .withColumn("aggregation_type", F.lit("daily_batch"))
        .withColumn("processed_at", F.current_timestamp())
    )

    # Gold aggregation write - use append mode to allow stream data to coexist
    try:
        (
            agg
            .repartition(4, "FL_DATE")
            .write
            .mode("append")
            .partitionBy("FL_DATE")
            .parquet(GOLD_OUTPUT)
        )
    except Exception:
        import traceback
        err = traceback.format_exc()
        print("[ERROR] Gold parquet write failed", file=sys.stderr)
        print(err, file=sys.stderr)
        try:
            log_path = f"gs://{BUCKET_NAME}/logs/gold_parquet_error_{int(__import__('time').time())}.log"
            spark.sparkContext.parallelize([err]).saveAsTextFile(log_path)
            print(f"[INFO] Gold write error logged to {log_path}")
        except Exception:
            print("[WARN] Could not write gold error log to GCS", file=sys.stderr)

    # BigQuery write
    safe_write_bigquery(
        spark=spark,
        df=agg,
        table=BQ_TABLE,
        temp_bucket=BQ_TEMP_BUCKET,
        bucket_name=BUCKET_NAME
    )

    spark.stop()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession, functions as F, types as T

# --------------------------------------------------------
# Hardcoded config
# --------------------------------------------------------
PROJECT_ID = "jads-de-assignment-2"
BUCKET_NAME = "airline-delays-bucket"
DATASET = "de_flights"

STREAM_INPUT = f"gs://{BUCKET_NAME}/stream/flights_in/"
AIRLINES_INPUT = f"gs://{BUCKET_NAME}/raw/airlines/airlines.csv"
AIRPORTS_INPUT = f"gs://{BUCKET_NAME}/raw/airports/airports.csv"

CHECKPOINT_BASE = f"gs://{BUCKET_NAME}/stream/checkpoints/flight_stream"
GOLD_OUTPUT = f"gs://{BUCKET_NAME}/gold/delay_stats/"  # Unified with batch

BQ_TABLE = f"{PROJECT_ID}.{DATASET}.delay_stats"  # Unified table for batch & stream
BQ_TEMP_BUCKET = "airline-temp-data"  # zonder gs://
CONSOLE_TRUNCATE = True

def safe_stream_to_bigquery(spark, df, table, temp_bucket, checkpoint, bucket_name):
    import traceback
    try:
        return (
            df.writeStream
            .format("bigquery")
            .option("table", table)
            .option("temporaryGcsBucket", temp_bucket)
            .option("checkpointLocation", checkpoint)
            .outputMode("append")
            .start()
        )
    except Exception:
        error_text = traceback.format_exc()

        print("[STREAM ERROR] BigQuery streaming sink failed!", file=sys.stderr)
        print(error_text, file=sys.stderr)

        # Also log to GCS
        try:
            log_path = f"gs://{bucket_name}/logs/bq_streaming_error.log"
            spark.sparkContext.parallelize([error_text]).saveAsTextFile(log_path)
        except:
            print("[WARN] Could not write stream error to GCS.", file=sys.stderr)

        return None


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
    # Conservative Spark configuration for streaming to reduce resource pressure.
    # These settings keep memory/cores low and make job behavior predictable.
    spark = (
        SparkSession.builder
        .appName("StreamFlightDelays")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "2")
        .config("spark.dynamicAllocation.enabled", "false")
        # Reduce shuffle parallelism for streaming aggregations
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.driver.maxResultSize", "512m")
        .config("spark.executor.memoryOverhead", "512")
        .getOrCreate()
    )

    flights_schema = get_flights_schema()

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
    
    # Normalize airport codes like in batch
    airports = (
        airports
        .withColumn("AIRPORT_CODE", F.trim(F.upper(F.col("AIRPORT_CODE"))))
        .withColumn("AIRPORT_NAME", F.trim(F.col("AIRPORT_NAME")))
        .withColumn("CITY_NAME", F.trim(F.col("CITY_NAME")))
        .withColumn("STATE_CODE", F.trim(F.col("STATE_CODE")))
    )
    
    # Normalize airlines
    airlines = (
        airlines
        .withColumn("AIRLINE_CODE", F.trim(F.upper(F.col("AIRLINE_CODE"))))
        .withColumn("AIRLINE_NAME", F.trim(F.col("AIRLINE_NAME")))
    )

    # Throttle file ingestion for streaming to avoid big bursts
    stream_df = (
        spark.readStream
        .option("header", "true")
        .option("maxFilesPerTrigger", 10)
        .schema(flights_schema)
        .csv(STREAM_INPUT)
    )

    # Normalize codes before filtering
    stream_df = (
        stream_df
        .withColumn("ORIGIN_AIRPORT", F.trim(F.upper(F.col("ORIGIN_AIRPORT"))))
        .withColumn("DESTINATION_AIRPORT", F.trim(F.upper(F.col("DESTINATION_AIRPORT"))))
        .withColumn("AIRLINE", F.trim(F.upper(F.col("AIRLINE"))))
    )
    
    stream_clean = (
        stream_df
        .filter(F.col("CANCELLED") == 0)
        .filter(F.col("DIVERTED") == 0)
        .filter(F.col("AIRLINE").isNotNull())
        .filter(F.col("ORIGIN_AIRPORT").isNotNull())
        .filter(F.col("DEPARTURE_DELAY").isNotNull())
        .filter(F.col("DEPARTURE_DELAY") > -60)
        .filter(F.col("DEPARTURE_DELAY") < 1000)
    )

    stream_with_ts = (
        stream_clean
        .withColumn(
            "event_ts",
            F.to_timestamp(
                F.format_string(
                    "%04d-%02d-%02d %02d:%02d:00",
                    F.col("YEAR"),
                    F.col("MONTH"),
                    F.col("DAY"),
                    (F.col("SCHEDULED_DEPARTURE") / 100).cast("int"),
                    (F.col("SCHEDULED_DEPARTURE") % 100).cast("int"),
                )
            )
        )
        .filter(F.col("event_ts").isNotNull())
    )

    stream_enriched = (
        stream_with_ts
        .join(
            airlines,
            stream_with_ts["AIRLINE"] == airlines["AIRLINE_CODE"],
            how="left"
        )
        .join(
            airports.select(
                F.col("AIRPORT_CODE").alias("ORIGIN_AIRPORT_CODE"),
                F.col("AIRPORT_NAME").alias("ORIGIN_AIRPORT_NAME"),
                F.col("CITY_NAME").alias("ORIGIN_CITY"),
                F.col("STATE_CODE").alias("ORIGIN_STATE")
            ),
            stream_with_ts["ORIGIN_AIRPORT"] == F.col("ORIGIN_AIRPORT_CODE"),
            how="left"
        )
    )
    
    # Add business logic columns for dashboarding (same as batch)
    stream_enriched = (
        stream_enriched
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
            "DEP_HOUR",
            (F.col("SCHEDULED_DEPARTURE") / 100).cast("int")
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
    )

    windowed = (
        stream_enriched
        .withWatermark("event_ts", "30 minutes")
        .groupBy(
            F.window("event_ts", "15 minutes", "5 minutes").alias("time_window"),
            F.col("FL_DATE"),
            F.col("AIRLINE"),
            F.col("AIRLINE_NAME"),
            F.col("ORIGIN_AIRPORT"),
            F.col("ORIGIN_AIRPORT_NAME"),
            F.col("ORIGIN_CITY"),
            F.col("ORIGIN_STATE")
        )
        .agg(
            # Core metrics (matching batch)
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
        .withColumn("window_start", F.col("time_window").start)
        .withColumn("window_end", F.col("time_window").end)
        .withColumn("aggregation_type", F.lit("streaming_15min"))
        .withColumn("processed_at", F.current_timestamp())
        .drop("time_window")
    )

    parquet_checkpoint = CHECKPOINT_BASE.rstrip("/") + "/gold_parquet"
    bq_checkpoint = CHECKPOINT_BASE.rstrip("/") + "/bq"

    console_query = (
        windowed
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", str(CONSOLE_TRUNCATE).lower())
        .option("numRows", 20)
        .start()
    )

    # Parquet sink to unified GOLD layer (same as batch)
    parquet_query = (
        windowed
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", GOLD_OUTPUT)
        .option("checkpointLocation", parquet_checkpoint)
        .option("maxRecordsPerFile", 100000)
        .partitionBy("FL_DATE")
        .start()
    )

    bq_query = safe_stream_to_bigquery(
        spark=spark,
        df=windowed,
        table=BQ_TABLE,
        temp_bucket=BQ_TEMP_BUCKET,
        checkpoint=bq_checkpoint,
        bucket_name=BUCKET_NAME
    )


    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

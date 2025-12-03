from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET_NAME   = "airline-delays-bucket"
SILVER_OUTPUT = f"gs://{BUCKET_NAME}/silver/test_write/"

def main():
    spark = (
        SparkSession.builder
        .appName("TestGcsWrite")
        .getOrCreate()
    )

    df = spark.range(5).withColumn("msg", F.lit("hello"))
    df.show()

    (
        df.write
        .mode("overwrite")
        .parquet(SILVER_OUTPUT)
    )

    print(f"[INFO] Test write succeeded to {SILVER_OUTPUT}")

    spark.stop()

if __name__ == "__main__":
    main()

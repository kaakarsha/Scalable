import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

argv = sys.argv[1:]
S3_IN = argv[0] if len(argv) > 0 else os.getenv("S3_RAW_PATH", "s3://earthquake-pipeline-raw/data/")
S3_OUT = argv[1] if len(argv) > 1 else os.getenv("S3_BATCH_PATH", "s3://earthquake-pipeline-batch/output/")
TOP_N = int(os.getenv("TOP_N", "20"))


def run(spark):
    df = spark.read.json(S3_IN).cache()
    print(f"records={df.count()}")

    (
        df.groupBy("region")
        .agg(
            F.count("*").alias("events"),
            F.avg("mag").alias("avg_mag"),
            F.max("mag").alias("max_mag"),
        )
        .orderBy(F.desc("events"))
        .limit(TOP_N)
        .write.mode("overwrite")
        .parquet(f"{S3_OUT}region_stats/")
    )

    (
        df.withColumn(
            "mag_band",
            F.when(F.col("mag") < 3, "lt3")
            .when(F.col("mag") < 5, "3to5")
            .otherwise("gte5"),
        )
        .groupBy("mag_band")
        .agg(F.count("*").alias("events"))
        .write.mode("overwrite")
        .parquet(f"{S3_OUT}mag_bands/")
    )

    (
        df.withColumn(
            "hour",
            F.from_unixtime((F.col("time") / 1000).cast("long"), "yyyy-MM-dd HH"),
        )
        .groupBy("hour")
        .agg(F.count("*").alias("events"), F.avg("mag").alias("avg_mag"))
        .orderBy("hour")
        .write.mode("overwrite")
        .parquet(f"{S3_OUT}hourly/")
    )

    df.unpersist()
    print(f"batch out={S3_OUT}")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("earthquake-batch")
        .config("spark.sql.shuffle.partitions", "100")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run(spark)
    spark.stop()

import argparse
import os
import sys
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F


if os.name == "nt":  # Windows only: local fix, not needed on the cloud
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def dedup_data_latest(df):
    """Keep only the latest record per order_id."""
    w = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc(),
        F.col("qty").desc(),  # tiebreaker
    )
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def run(spark, input_path, output_path):
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )
    result = dedup_data_latest(df)
    result.write.mode("overwrite").parquet(output_path)
    return result.count()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("dedupe-pipeline").getOrCreate()
    rows = run(spark, args.input, args.output)
    print(f"Wrote {rows} rows to {args.output}")


if __name__ == "__main__":
    main()
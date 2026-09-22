from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def dedup_data_latest(df):
    '''
    Keep only the latest record for order_id column
    '''
    w = Window.partitionBy('order_id').orderBy(
        F.col("updated_at").desc(),
        F.col("qty").desc()   # tiebreaker
    )

    dedup_df = (
        df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col('rn')==1)
        .drop("rn")
    )

    return dedup_df


if __name__ == "__main__":

    spark = SparkSession.builder.master('local[1]').appName("Fact Sales Testing").getOrCreate()

    data = [
        ("CUS-01", 10, "2026-09-20 09:00"),
        ("CUS-01", 12, "2026-09-20 11:00"),  # corrected later, should win
        ("CUS-02", 5,  "2026-09-20 09:30"),
    ]
    columns = ["order_id", "qty", "updated_at"]

    df = spark.createDataFrame(data, columns)

    print("Before:")
    df.show()
    print("After:")

    dedup_df = dedup_data_latest(df)
    dedup_df.show()

    spark.stop()
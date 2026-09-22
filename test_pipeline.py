import pytest
from pyspark.sql import SparkSession
from pipeline import dedup_data_latest

@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield session
    session.stop()

def test_dedupe_keeps_latest_record(spark):
    df = spark.createDataFrame(
        [
            ("O-1", 10, "2026-09-20 09:00"),
            ("O-1", 12, "2026-09-20 11:00"),
        ],
        ["order_id", "qty", "updated_at"],
    )

    result = dedup_data_latest(df).collect()

    assert len(result) == 1
    assert result[0].qty == 12



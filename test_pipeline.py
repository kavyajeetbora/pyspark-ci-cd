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


def test_exact_duplicates_become_one_row(spark):
    df = spark.createDataFrame(
        [
            ("O-1", 10, "2026-09-20 09:00"),
            ("O-1", 10, "2026-09-20 09:00"),
        ],
        ["order_id", "qty", "updated_at"],
    )
    assert dedup_data_latest(df).count() == 1

def test_null_timestamp_does_not_win(spark):
    df = spark.createDataFrame(
        [
            ("O-1", 10, None),
            ("O-1", 12, "2026-09-20 11:00"),
        ],
        ["order_id", "qty", "updated_at"],
    )
    result = dedup_data_latest(df).collect()
    assert result[0].qty == 12


def test_tie_on_timestamp_is_deterministic(spark):
    rows = [
        ("O-1", 10, "2026-09-20 09:00"),
        ("O-1", 12, "2026-09-20 09:00"),  # same timestamp
    ]
    cols = ["order_id", "qty", "updated_at"]

    first = dedup_data_latest(spark.createDataFrame(rows, cols)).collect()[0]
    reversed_input = dedup_data_latest(spark.createDataFrame(rows[::-1], cols)).collect()[0]

    assert first.qty == reversed_input.qty


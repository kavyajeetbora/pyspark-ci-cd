# Pytest for PySpark: Notes

## What we test
Only the transformation logic (e.g. `dedupe_latest`), using tiny hand-made DataFrames.
The `assert` lines are the actual test.

## How pytest finds tests
`pytest` scans the current folder and subfolders by naming convention:

| What | Rule | Example |
|---|---|---|
| Files | `test_*.py` or `*_test.py` | `test_pipeline.py` |
| Functions | start with `test_` | `test_dedupe_keeps_latest_record` |
| Classes | start with `Test` | `TestDedupe` |

Anything else (like `dedupe_latest` or a fixture) is not treated as a test.

## Fixtures
A fixture is reusable setup that tests ask for. It is not a grouping mechanism.

```python
@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield session      # before yield = setup, after yield = teardown
    session.stop()
```

- A test requests it by **parameter name**: `def test_x(spark):`. pytest injects it (dependency injection).
- `scope="session"`: Spark starts once for all tests, not once per test.
- The function under test doesn't need Spark. The **test** needs it to build input DataFrames.

**Why not a global SparkSession?** It starts at import time (even when only listing tests),
has no reliable teardown, and gives no control over lifetime. Fixtures are the standard.

## Test structure
```python
def test_dedupe_keeps_latest_record(spark):
    df = spark.createDataFrame([...], [...])   # 1. tiny input
    result = dedupe_latest(df).collect()       # 2. call the function
    assert result[0].qty == 12                 # 3. check the output
```

## Reading results
- **PASSED**: logic works
- **FAILED**: an `assert` didn't hold, so the logic is wrong
- **ERROR**: setup broke (Spark/Java/environment), so the logic was never checked

## Commands
```
pytest -v               # run all tests, one line per test
pytest --collect-only   # list discovered tests without running them
```

## Windows gotcha
Spark looks for `python3`, which doesn't exist on Windows. Fix at the top of `pipeline.py`:
```python
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```
`winutils` / temp-dir warnings are harmless for this setup.

## Proving tests work
Break the logic on purpose (`.desc()` → `.asc()`), and the test should fail. Revert, and it passes.
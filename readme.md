The repo I gave you already has this, but here's the simplest version to understand and build first. It comes down to four pieces.

**1. Structure the code so it's testable.** Separate logic from I/O:
- `transformations.py`: pure functions (DataFrame in, DataFrame out)
- `job.py`: reads input, calls the functions, writes output

**2. Write a few tests.** Use pytest with a local SparkSession and tiny DataFrames:

```python
def test_dedupe_keeps_latest(spark):
    df = spark.createDataFrame(
        [("O-1", 10, "09:00"), ("O-1", 12, "11:00")],
        ["order_id", "qty", "updated_at"])
    assert dedupe_latest(df).collect()[0].qty == 12
```

**3. CI: run the tests on every pull request.** Put this in `.github/workflows/ci.yml`:

```yaml
name: CI
on: pull_request
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.10" }
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: "17" }
      - run: pip install pyspark pytest ruff
      - run: ruff check .
      - run: pytest
```

Then turn on branch protection on `main` so a PR can't merge unless this passes.

**4. CD: deploy on merge to main.** Add a second workflow that runs `on: push: branches: [main]` and does three things:
1. Logs in to Azure (`azure/login`)
2. Uploads the code to storage (`az storage blob upload-batch`)
3. Triggers the Spark job (`az synapse spark job submit`), or lets the existing schedule pick up the new code

**Build it in this order:**
1. Tests pass locally
2. CI workflow blocks bad PRs
3. CD to dev
4. A manual approval step, then prod

Steps 1 and 2 alone are a complete, working CI setup. You can do them in an hour, and they give you a real story to tell in the interview.
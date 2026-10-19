from pathlib import Path

import pytest

DAY03_FILE = Path("data/landing/paysim/day=03/paysim_day03.csv")


@pytest.mark.skipif(not DAY03_FILE.exists(), reason="local PaySim split files not available")
def test_spark_reads_a_daily_file(spark):
    df = spark.read.option("header", True).option("inferSchema", True).csv(str(DAY03_FILE))
    assert df.count() == 1070
    assert len(df.columns) == 11


def test_delta_write_and_read(spark, tmp_path):
    rows = [(1, "TRANSFER", 181.0, 1), (1, "PAYMENT", 9839.64, 0)]
    df = spark.createDataFrame(rows, ["step", "type", "amount", "isFraud"])

    path = str(tmp_path / "delta_smoke")
    df.write.format("delta").save(path)
    back = spark.read.format("delta").load(path)

    assert back.count() == 2
    assert back.filter("isFraud = 1").count() == 1
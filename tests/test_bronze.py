from pathlib import Path

import pytest
from transforms.bronze import BRONZE_SCHEMA, SOURCE_COLUMNS, add_bronze_metadata, read_batch_files

LANDING = Path("data/landing/paysim")


def make_raw_rows(spark):
    row = ("1", "TRANSFER", "181.0", "C1", "181.0", "0.0", "C2", "0.0", "0.0", "1", "0")
    return spark.createDataFrame([row], schema=BRONZE_SCHEMA)


def test_bronze_schema_is_all_strings():
    assert [f.name for f in BRONZE_SCHEMA.fields] == SOURCE_COLUMNS
    assert all(f.dataType.simpleString() == "string" for f in BRONZE_SCHEMA.fields)


def test_add_bronze_metadata_adds_lineage_columns(spark):
    out = add_bronze_metadata(make_raw_rows(spark), "batch")
    assert "_ingest_ts" in out.columns and "_ingest_path" in out.columns
    row = out.first()
    assert row["_ingest_path"] == "batch"
    assert row["_ingest_ts"] is not None
    assert row["amount"] == "181.0"  # source values untouched


def test_add_bronze_metadata_rejects_unknown_path(spark):
    with pytest.raises(ValueError):
        add_bronze_metadata(make_raw_rows(spark), "manual")


@pytest.mark.skipif(not LANDING.exists(), reason="local landing files not available")
def test_read_batch_files_on_one_day(spark):
    df = read_batch_files(spark, str(LANDING / "day=03"))
    assert df.count() == 1070
    assert "day" not in df.columns
    assert df.first()["_source_file"].endswith("paysim_day03.csv")
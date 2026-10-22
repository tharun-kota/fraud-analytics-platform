import datetime

from transforms.bronze import BRONZE_SCHEMA, add_bronze_metadata
from transforms.gold import (
     build_dim_account, build_dim_date, build_dim_time,
    build_dim_transaction_type, build_fact_transactions,
)
from transforms.silver import transform_silver

ROWS = [
    ("1", "TRANSFER", "181.0", "C1", "181.0", "0.0", "C2", "0.0", "0.0", "1", "0"),
    ("2", "CASH_OUT", "181.0", "C2", "181.0", "0.0", "C3", "0.0", "181.0", "1", "0"),
    ("51", "PAYMENT", "50.0", "C1", "500.0", "450.0", "M1", "0.0", "0.0", "0", "0"),
]


def silver(spark):
    bronze = add_bronze_metadata(spark.createDataFrame(ROWS, schema=BRONZE_SCHEMA), "batch")
    return transform_silver(bronze)


def test_dim_time_has_24_hours_with_bands(spark):
    rows = {r["hour"]: r["time_band"] for r in build_dim_time(spark).collect()}
    assert len(rows) == 24
    assert (rows[3], rows[9], rows[14], rows[20]) == ("night", "morning", "afternoon", "evening")


def test_dim_transaction_type_eligibility(spark):
    rows = {r["txn_type"]: r["is_fraud_eligible"] for r in build_dim_transaction_type(spark).collect()}
    assert len(rows) == 5
    assert {t for t, eligible in rows.items() if eligible} == {"TRANSFER", "CASH_OUT"}


def test_dim_date_keys_and_weekends(spark):
    rows = {r["date_key"]: r for r in build_dim_date(silver(spark)).collect()}
    assert set(rows) == {20260101, 20260103}
    assert rows[20260101]["date"] == datetime.date(2026, 1, 1)
    assert rows[20260101]["is_weekend"] is False   # Thursday
    assert rows[20260103]["is_weekend"] is True    # Saturday


def test_dim_account_one_row_per_account(spark):
    rows = {r["account_id"]: r for r in build_dim_account(silver(spark)).collect()}
    assert set(rows) == {"C1", "C2", "C3", "M1"}
    assert (rows["C1"]["txn_count_out"], rows["C1"]["txn_count_in"]) == (2, 0)
    assert (rows["C2"]["txn_count_out"], rows["C2"]["txn_count_in"]) == (1, 1)
    assert rows["M1"]["account_type"] == "merchant"


def test_fact_matches_silver_and_links_to_dimensions(spark):
    s = silver(spark)
    fact = build_fact_transactions(s, build_dim_transaction_type(spark))
    assert fact.count() == s.count()
    assert fact.filter("type_key IS NULL OR date_key IS NULL").count() == 0

    accounts = build_dim_account(s)
    linked = fact.join(accounts, fact.orig_account_key == accounts.account_key)
    assert linked.count() == fact.count()
    #assert account_key  # helper is importable for the dashboard model docs
from decimal import Decimal

from transforms.bronze import BRONZE_SCHEMA, add_bronze_metadata
from transforms.silver import transform_silver, transform_silver_core
ROWS = [
    ("1", "TRANSFER", "181.0", "C1", "181.0", "0.0", "C2", "0.0", "0.0", "1", "0"),
    ("25", "PAYMENT", "9839.64", "C3", "170136.0", "160296.36", "M1", "0.0", "0.0", "0", "0"),
    ("743", "CASH_OUT", "100.5", "C4", "200.0", "99.5", "C5", "10.0", "110.5", "0", "0"),
    ("2", "TRANSFER", "abc", "C6", "50.0", "0.0", "C7", "0.0", "0.0", "0", "0"),
]


def silver(spark):
    bronze = add_bronze_metadata(spark.createDataFrame(ROWS, schema=BRONZE_SCHEMA), "batch")
    return transform_silver_core(bronze)


def by_orig(df):
    return {r["orig_account_id"]: r for r in df.collect()}


def test_types_and_names(spark):
    types = dict(silver(spark).dtypes)
    assert types["step"] == "int"
    assert types["amount"] == "decimal(18,2)"
    assert types["is_fraud"] == "int"
    assert "nameOrig" not in types and "orig_account_id" in types


def test_event_time_from_step(spark):
    rows = by_orig(silver(spark))
    assert (str(rows["C1"]["event_date"]), rows["C1"]["event_hour"]) == ("2026-01-01", 0)
    assert (str(rows["C3"]["event_date"]), rows["C3"]["event_hour"]) == ("2026-01-02", 0)
    assert (str(rows["C4"]["event_date"]), rows["C4"]["event_hour"]) == ("2026-01-31", 22)


def test_merchant_balances_are_null(spark):
    rows = by_orig(silver(spark))
    assert rows["C3"]["dest_account_type"] == "merchant"
    assert rows["C3"]["dest_balance_before"] is None
    assert rows["C4"]["dest_account_type"] == "customer"
    assert rows["C4"]["dest_balance_before"] == Decimal("10.00")


def test_transaction_id_is_deterministic_and_unique(spark):
    first = {r["orig_account_id"]: r["transaction_id"] for r in silver(spark).collect()}
    second = {r["orig_account_id"]: r["transaction_id"] for r in silver(spark).collect()}
    assert first == second
    assert len(set(first.values())) == len(ROWS)
    assert all(len(tx_id) == 64 for tx_id in first.values())


def test_invalid_amount_becomes_null_not_error(spark):
    rows = by_orig(silver(spark))
    assert rows["C6"]["amount"] is None
    
    
SIGNAL_ROWS = [
    # orig: full-balance transfer (classic fraud)
    ("1", "TRANSFER", "181.0", "F1", "181.0", "0.0", "C2", "0.0", "0.0", "1", "0"),
    # orig: 10M cap from an account holding more
    ("4", "TRANSFER", "10000000.0", "F2", "12930418.44", "2930418.44", "C3", "0.0", "0.0", "1", "0"),
    # orig: normal partial cash-out, balances add up
    ("9", "CASH_OUT", "100.5", "L1", "200.0", "99.5", "C4", "10.0", "110.5", "0", "0"),
    # orig: sent from a zero balance; receiver stays at 0
    ("1", "CASH_OUT", "416001.33", "Q1", "0.0", "0.0", "C5", "0.0", "0.0", "1", "0"),
    # orig: a payment that spends the whole balance (not an eligible type)
    ("3", "PAYMENT", "50.0", "P1", "50.0", "0.0", "M9", "0.0", "0.0", "0", "0"),
]


def signals(spark):
    bronze = add_bronze_metadata(spark.createDataFrame(SIGNAL_ROWS, schema=BRONZE_SCHEMA), "batch")
    return by_orig(transform_silver(bronze))


def test_full_balance_transfer_is_flagged(spark):
    row = signals(spark)["F1"]
    assert row["amount_is_full_balance"] is True
    assert row["orig_drained"] is True
    assert row["rule_flag"] == 1


def test_ten_million_cap_is_flagged(spark):
    row = signals(spark)["F2"]
    assert row["amount_is_full_balance"] is False
    assert row["rule_flag"] == 1


def test_normal_cash_out_is_not_flagged(spark):
    row = signals(spark)["L1"]
    assert row["rule_flag"] == 0
    assert row["orig_balance_error"] is False
    assert row["dest_balance_error"] is False


def test_balance_errors_and_unchanged_receiver(spark):
    row = signals(spark)["Q1"]
    assert row["orig_balance_error"] is True
    assert row["dest_balance_unchanged"] is True
    assert row["rule_flag"] == 0


def test_payment_is_never_flagged_and_merchant_checks_are_null(spark):
    row = signals(spark)["P1"]
    assert row["amount_is_full_balance"] is True
    assert row["rule_flag"] == 0
    assert row["dest_balance_error"] is None
    assert row["dest_balance_unchanged"] is False
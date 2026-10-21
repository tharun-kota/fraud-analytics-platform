from quality.rules import apply_hard_rules
from transforms.bronze import BRONZE_SCHEMA, add_bronze_metadata
from transforms.silver import transform_silver

GOOD = ("1", "TRANSFER", "181.0", "C1", "181.0", "0.0", "C2", "0.0", "0.0", "1", "0")
BAD_AMOUNT = ("2", "TRANSFER", "abc", "C3", "50.0", "0.0", "C4", "0.0", "0.0", "0", "0")
BAD_STEP_AND_TYPE = ("999", "REFUND", "10.0", "C5", "50.0", "40.0", "C6", "0.0", "0.0", "0", "0")


def split(spark, rows):
    bronze = add_bronze_metadata(spark.createDataFrame(rows, schema=BRONZE_SCHEMA), "batch")
    return apply_hard_rules(transform_silver(bronze))


def test_good_row_is_valid(spark):
    valid, quarantine = split(spark, [GOOD])
    assert valid.count() == 1 and quarantine.count() == 0
    assert "dq_rule_failed" not in valid.columns


def test_unparseable_amount_is_quarantined(spark):
    valid, quarantine = split(spark, [GOOD, BAD_AMOUNT])
    assert valid.count() == 1
    assert quarantine.first()["dq_rule_failed"] == "invalid_amount"


def test_all_failed_rules_are_listed(spark):
    _, quarantine = split(spark, [BAD_STEP_AND_TYPE])
    failed = quarantine.first()["dq_rule_failed"].split(",")
    assert set(failed) == {"step_out_of_range", "unknown_type"}
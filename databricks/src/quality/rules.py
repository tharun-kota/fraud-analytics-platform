"""Hard data quality rules for silver. Rows failing any rule go to quarantine."""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

VALID_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
MAX_STEP = 743


def hard_rules() -> dict[str, Column]:
    """Each rule is a condition that must be TRUE for a valid row."""
    return {
        "step_out_of_range": F.col("step").between(1, MAX_STEP),
        "unknown_type": F.col("txn_type").isin(VALID_TYPES),
        "invalid_amount": F.col("amount") >= 0,
        "invalid_orig_account": F.col("orig_account_id").startswith("C"),
        "invalid_dest_account": F.col("dest_account_id").startswith("C") | F.col("dest_account_id").startswith("M"),
        "invalid_fraud_label": F.col("is_fraud").isin(0, 1),
        "invalid_legacy_flag": F.col("is_flagged_legacy").isin(0, 1),
    }


def apply_hard_rules(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows into (valid, quarantine). Quarantine rows list the rules they failed."""
    failures = [
        F.when(~F.coalesce(condition, F.lit(False)), F.lit(name))
        for name, condition in hard_rules().items()
    ]
    checked = df.withColumn("dq_rule_failed", F.concat_ws(",", *failures))
    valid = checked.filter(F.col("dq_rule_failed") == "").drop("dq_rule_failed")
    quarantine = checked.filter(F.col("dq_rule_failed") != "")
    return valid, quarantine
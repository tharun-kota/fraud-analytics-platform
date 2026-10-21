"""Silver layer, part 1: typed, renamed and time-enriched PaySim transactions."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ANCHOR_DATE = "2026-01-01"  # assumption: step 1 = 2026-01-01 00:00 UTC
MONEY = "decimal(18,2)"


def cast_and_rename(df: DataFrame) -> DataFrame:
    """Convert bronze text columns to proper types and snake_case names."""
    return df.select(
        F.col("step").try_cast("int").alias("step"),
        F.col("type").alias("txn_type"),
        F.col("amount").try_cast(MONEY).alias("amount"),
        F.col("nameOrig").alias("orig_account_id"),
        F.col("oldbalanceOrg").try_cast(MONEY).alias("orig_balance_before"),
        F.col("newbalanceOrig").try_cast(MONEY).alias("orig_balance_after"),
        F.col("nameDest").alias("dest_account_id"),
        F.col("oldbalanceDest").try_cast(MONEY).alias("dest_balance_before"),
        F.col("newbalanceDest").try_cast(MONEY).alias("dest_balance_after"),
        F.col("isFraud").try_cast("int").alias("is_fraud"),
        F.col("isFlaggedFraud").try_cast("int").alias("is_flagged_legacy"),
        "_ingest_ts",
        "_ingest_path",
    )


def add_time_columns(df: DataFrame) -> DataFrame:
    """Derive event date, hour and timestamp from the hourly step."""
    offset = F.col("step") - 1
    return (
        df.withColumn("event_hour", (offset % 24).cast("int"))
          .withColumn(
              "event_date",
              F.date_add(F.lit(ANCHOR_DATE).cast("date"), F.floor(offset / 24).cast("int")),
          )
          .withColumn(
              "event_ts",
              F.make_timestamp(
                  F.year("event_date"), F.month("event_date"), F.dayofmonth("event_date"),
                  F.col("event_hour"), F.lit(0), F.lit(0),
              ),
          )
    )


def add_account_columns(df: DataFrame) -> DataFrame:
    """Label the destination type and null out merchant balances (never recorded)."""
    is_merchant = F.col("dest_account_id").startswith("M")
    no_balance = F.lit(None).cast(MONEY)
    return (
        df.withColumn("dest_account_type", F.when(is_merchant, "merchant").otherwise("customer"))
          .withColumn("dest_balance_before", F.when(is_merchant, no_balance).otherwise(F.col("dest_balance_before")))
          .withColumn("dest_balance_after", F.when(is_merchant, no_balance).otherwise(F.col("dest_balance_after")))
    )


def add_transaction_id(df: DataFrame) -> DataFrame:
    """Deterministic ID: the same transaction always gets the same ID."""
    key = F.concat_ws(
        "|",
        F.col("step").cast("string"), F.col("txn_type"), F.col("amount").cast("string"),
        F.col("orig_account_id"), F.col("dest_account_id"),
    )
    return df.withColumn("transaction_id", F.sha2(key, 256))


def transform_silver_core(bronze: DataFrame) -> DataFrame:
    """Part 1 of silver: types, names, time, accounts and ID."""
    df = cast_and_rename(bronze)
    df = add_time_columns(df)
    df = add_account_columns(df)
    return add_transaction_id(df)
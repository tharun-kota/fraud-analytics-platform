"""Gold layer: star schema for Power BI (dimensions and fact table)."""
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from transforms.silver import ANCHOR_DATE, FRAUD_ELIGIBLE_TYPES

TRANSACTION_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]


def account_key(col: Column) -> Column:
    """Stable surrogate key: the same account ID always gives the same key."""
    return F.xxhash64(col)


def build_dim_date(silver: DataFrame) -> DataFrame:
    return (
        silver.select(F.col("event_date").alias("date")).distinct()
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("day_of_month", F.dayofmonth("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
        .withColumn(
            "week_of_period",
            (F.floor(F.datediff("date", F.lit(ANCHOR_DATE).cast("date")) / 7) + 1).cast("int"),
        )
        .select("date_key", "date", "day_of_month", "day_name", "is_weekend", "week_of_period")
    )


def build_dim_time(spark: SparkSession) -> DataFrame:
    hour = F.col("id").cast("int")
    band = (
        F.when(hour < 6, "night")
         .when(hour < 12, "morning")
         .when(hour < 18, "afternoon")
         .otherwise("evening")
    )
    return spark.range(24).select(hour.alias("hour_key"), hour.alias("hour"), band.alias("time_band"))


def build_dim_transaction_type(spark: SparkSession) -> DataFrame:
    rows = [(i + 1, t, t in FRAUD_ELIGIBLE_TYPES) for i, t in enumerate(TRANSACTION_TYPES)]
    return spark.createDataFrame(rows, "type_key int, txn_type string, is_fraud_eligible boolean")


def build_dim_account(silver: DataFrame) -> DataFrame:
    outgoing = silver.groupBy(F.col("orig_account_id").alias("account_id")).agg(
        F.count("*").alias("txn_count_out"),
        F.min("event_ts").alias("first_out"),
        F.max("event_ts").alias("last_out"),
    )
    incoming = silver.groupBy(F.col("dest_account_id").alias("account_id")).agg(
        F.count("*").alias("txn_count_in"),
        F.min("event_ts").alias("first_in"),
        F.max("event_ts").alias("last_in"),
    )
    return (
        outgoing.join(incoming, "account_id", "full_outer")
        .select(
            account_key(F.col("account_id")).alias("account_key"),
            "account_id",
            F.when(F.col("account_id").startswith("M"), "merchant").otherwise("customer").alias("account_type"),
            F.least("first_out", "first_in").alias("first_seen_ts"),
            F.greatest("last_out", "last_in").alias("last_seen_ts"),
            F.coalesce("txn_count_out", F.lit(0)).alias("txn_count_out"),
            F.coalesce("txn_count_in", F.lit(0)).alias("txn_count_in"),
        )
    )


def build_fact_transactions(silver: DataFrame, dim_type: DataFrame) -> DataFrame:
    return (
        silver.join(F.broadcast(dim_type.select("type_key", "txn_type")), "txn_type", "left")
        .select(
            "transaction_id",
            F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("event_hour").alias("hour_key"),
            "type_key",
            account_key(F.col("orig_account_id")).alias("orig_account_key"),
            account_key(F.col("dest_account_id")).alias("dest_account_key"),
            "amount",
            "orig_balance_error",
            "dest_balance_error",
            "orig_drained",
            "amount_is_full_balance",
            "dest_balance_unchanged",
            "rule_score",
            "rule_flag",
            "is_fraud",
            "is_flagged_legacy",
            F.col("_ingest_path").alias("ingest_path"),
        )
    )
    
RULES = {"rule_v1": "rule_flag", "legacy": "is_flagged_legacy"}


def build_agg_fraud_daily(fact: DataFrame) -> DataFrame:
    is_fraud = F.col("is_fraud") == 1
    return (
        fact.groupBy("date_key", "type_key")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("txn_amount"),
            F.sum(is_fraud.cast("int")).alias("fraud_count"),
            F.sum(F.when(is_fraud, F.col("amount")).otherwise(0)).alias("fraud_amount"),
        )
        .withColumn("fraud_rate", F.try_divide(F.col("fraud_count"), F.col("txn_count")))
    )


def build_agg_rule_performance(fact: DataFrame) -> DataFrame:
    fraud = F.col("is_fraud") == 1
    parts = []
    for rule_name, flag_column in RULES.items():
        flagged = F.col(flag_column) == 1
        parts.append(
            fact.groupBy("date_key")
            .agg(
                F.sum((flagged & fraud).cast("int")).alias("true_positives"),
                F.sum((flagged & ~fraud).cast("int")).alias("false_positives"),
                F.sum((~flagged & fraud).cast("int")).alias("false_negatives"),
            )
            .withColumn("rule_name", F.lit(rule_name))
        )
    combined = parts[0].unionByName(parts[1])
    tp, fp, fn = F.col("true_positives"), F.col("false_positives"), F.col("false_negatives")
    return (
        combined.withColumn("precision", F.try_divide(tp, tp + fp))
                .withColumn("recall", F.try_divide(tp, tp + fn))
                .select("date_key", "rule_name", "true_positives", "false_positives",
                        "false_negatives", "precision", "recall")
    )


def build_agg_account_risk(fact: DataFrame) -> DataFrame:
    return (
        fact.groupBy(F.col("orig_account_key").alias("account_key"))
        .agg(
            F.sum("is_fraud").alias("fraud_txn_count"),
            F.sum("rule_flag").alias("flagged_txn_count"),
            F.sum("amount").alias("total_out_amount"),
            F.sum(F.col("orig_drained").cast("int")).alias("drained_count"),
        )
        .filter((F.col("fraud_txn_count") > 0) | (F.col("flagged_txn_count") > 0))
    )
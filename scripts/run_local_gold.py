"""Build local gold tables (star schema + aggregates) from silver and check they reconcile."""
import time

from local_spark import get_spark
from pyspark.sql import functions as F

from transforms.gold import (  # noqa: E402
    build_agg_account_risk, build_agg_fraud_daily, build_agg_rule_performance,
    build_dim_account, build_dim_date, build_dim_time,
    build_dim_transaction_type, build_fact_transactions,
)

SILVER_PATH = "data/delta/silver/transactions"
GOLD_PATH = "data/delta/gold"


def write_table(df, name):
    path = f"{GOLD_PATH}/{name}"
    df.write.format("delta").mode("overwrite").save(path)
    return df.sparkSession.read.format("delta").load(path)


def total(df, column):
    return df.agg(F.sum(column)).first()[0]


if __name__ == "__main__":
    spark = get_spark("fraud-local-gold")
    start = time.time()
    silver = spark.read.format("delta").load(SILVER_PATH)

    # 1. Dimensions and fact table
    dim_type = write_table(build_dim_transaction_type(spark), "dim_transaction_type")
    tables = {
        "dim_date": write_table(build_dim_date(silver), "dim_date"),
        "dim_time": write_table(build_dim_time(spark), "dim_time"),
        "dim_transaction_type": dim_type,
        "dim_account": write_table(build_dim_account(silver), "dim_account"),
        "fact_transactions": write_table(build_fact_transactions(silver, dim_type), "fact_transactions"),
    }
    fact = tables["fact_transactions"]

    # 2. Aggregates (built from the fact table)
    tables["agg_fraud_daily"] = write_table(build_agg_fraud_daily(fact), "agg_fraud_daily")
    tables["agg_rule_performance"] = write_table(build_agg_rule_performance(fact), "agg_rule_performance")
    tables["agg_account_risk"] = write_table(build_agg_account_risk(fact), "agg_account_risk")

    # 3. Row counts
    for name, df in tables.items():
        print(f"{name:<22} {df.count():>12,} rows")

    # 4. Reconciliation checks (gold hard DQ rules)
    checks = {
        "fact rows = silver rows": fact.count() == silver.count(),
        "fraud count matches silver": total(fact, "is_fraud") == total(silver, "is_fraud"),
        "rule flag count matches silver": total(fact, "rule_flag") == total(silver, "rule_flag"),
        "no missing keys in fact": fact.filter("type_key IS NULL OR date_key IS NULL OR hour_key IS NULL").count() == 0,
        "daily aggregate fraud = fact fraud": total(tables["agg_fraud_daily"], "fraud_count") == total(fact, "is_fraud"),
    }
    for name, passed in checks.items():
        print(f"{'PASS' if passed else 'FAIL'}  {name}")
    print(f"Finished in {time.time() - start:.0f} seconds")
    spark.stop()

    if not all(checks.values()):
        raise SystemExit(1)
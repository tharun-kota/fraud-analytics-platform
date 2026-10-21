"""Build local silver and quarantine Delta tables from bronze, then check rule v1."""
import time

from local_spark import get_spark
from pyspark.sql import functions as F

from quality.rules import apply_hard_rules  # noqa: E402
from transforms.silver import transform_silver  # noqa: E402

BRONZE_PATH = "data/delta/bronze/paysim_transactions"
SILVER_PATH = "data/delta/silver/transactions"
QUARANTINE_PATH = "data/delta/silver/transactions_quarantine"

if __name__ == "__main__":
    spark = get_spark("fraud-local-silver")
    start = time.time()

    bronze = spark.read.format("delta").load(BRONZE_PATH)
    valid, quarantine = apply_hard_rules(transform_silver(bronze))
    valid = valid.dropDuplicates(["transaction_id"])

    valid.write.format("delta").mode("overwrite").save(SILVER_PATH)
    quarantine.write.format("delta").mode("overwrite").save(QUARANTINE_PATH)

    silver = spark.read.format("delta").load(SILVER_PATH)
    print(f"Silver rows: {silver.count():,}")
    print(f"Quarantined rows: {spark.read.format('delta').load(QUARANTINE_PATH).count():,}")

    check = silver.groupBy("rule_flag", "is_fraud").count().orderBy("rule_flag", "is_fraud")
    check.show()
    false_alarms = silver.filter((F.col("rule_flag") == 1) & (F.col("is_fraud") == 0)).count()
    print(f"False alarms (flagged but legitimate): {false_alarms:,}")
    print(f"Finished in {time.time() - start:.0f} seconds")
    spark.stop()
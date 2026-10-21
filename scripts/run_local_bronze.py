"""Load the 29 daily landing files into a local bronze Delta table."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "databricks" / "src"))

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from transforms.bronze import add_bronze_metadata, read_batch_files  # noqa: E402

LANDING = "data/landing/paysim"
BRONZE_PATH = "data/delta/bronze/paysim_transactions"


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("fraud-local-bronze")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


if __name__ == "__main__":
    spark = get_spark()
    start = time.time()

    bronze = add_bronze_metadata(read_batch_files(spark, LANDING), "batch")
    bronze.write.format("delta").mode("overwrite").save(BRONZE_PATH)

    count = spark.read.format("delta").load(BRONZE_PATH).count()
    print(f"Bronze rows written: {count:,} in {time.time() - start:.0f} seconds")
    spark.stop()
"""Bronze layer: raw PaySim rows stored as strings, plus lineage columns."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

SOURCE_COLUMNS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
]
BRONZE_SCHEMA = StructType([StructField(c, StringType(), True) for c in SOURCE_COLUMNS])
VALID_INGEST_PATHS = {"batch", "stream"}


def add_bronze_metadata(df: DataFrame, ingest_path: str) -> DataFrame:
    """Add load time and ingest path. Shared by the local runner and Databricks."""
    if ingest_path not in VALID_INGEST_PATHS:
        raise ValueError(f"ingest_path must be one of {VALID_INGEST_PATHS}, got {ingest_path!r}")
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_ingest_path", F.lit(ingest_path))
    )


def read_batch_files(spark: SparkSession, landing_path: str) -> DataFrame:
    """Read daily CSV files as strings and record each row's source file (local runs)."""
    df = (
        spark.read.schema(BRONZE_SCHEMA)
        .option("header", True)
        .csv(landing_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    return df.drop("day") if "day" in df.columns else df
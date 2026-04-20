import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, BooleanType, ArrayType
)

MCC_LANDING_PATH       = "s3://fraud-demo-bucket-043309328060-us-east-1-an/reference/mcc_codes/"

MCC_SCHEMA = StructType([
    StructField("mcc_code",             StringType(), False),
    StructField("description",          StringType(), True),
    StructField("category",             StringType(), True),
    StructField("risk_level",           StringType(), True),
    StructField("requires_enhanced_dd", StringType(), True),
    StructField("effective_date",       StringType(), True),
    StructField("last_updated",         StringType(), True),
])

VALID_RISK_LEVELS = ["LOW","MEDIUM","HIGH"]
VALID_ENHANCED_DD = ["Y","N"]

@dlt.table(
    name    = "mcc_codes_raw",
    comment = "Bronze: MCC merchant category codes, auto-loaded from S3 CSV",
    table_properties = {
        "quality":                        "bronze",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect("valid_mcc_code",
            "mcc_code IS NOT NULL")
@dlt.expect("valid_risk_level",
            "risk_level IN ('LOW','MEDIUM','HIGH')")
@dlt.expect("valid_enhanced_dd",
            "requires_enhanced_dd IN ('Y','N')")
@dlt.expect("valid_description",
            "description IS NOT NULL")
def mcc_codes_raw():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",           "csv")
             .option("cloudFiles.schemaLocation",
                     "dbfs:/checkpoints/bronze/mcc_codes/schema")
             .option("cloudFiles.inferColumnTypes",  "false")
             .option("cloudFiles.validateOptions",   "true")
             .option("header",                       "true")
             .option("sep",                          ",")
             .option("quote",                        '"')
             .option("escape",                       '"')
             .option("encoding",                     "UTF-8")
             .option("nullValue",                    "")
             .schema(MCC_SCHEMA)
             .load(MCC_LANDING_PATH)
             .withColumn("source_file",
                 F.col("_metadata.file_path"))
             .withColumn("source_file_modified_ts",
                 F.col("_metadata.file_modification_time"))
             .withColumn("ingestion_ts",
                 F.current_timestamp())
             .withColumn("ingestion_date",
                 F.to_date(F.current_timestamp()))
             .withColumn("pipeline_version",
                 F.lit("1.0.0"))
    )


@dlt.table(
    name    = "mcc_codes_quarantine",
    comment = "Bronze: MCC rows failing quality rules",
    table_properties = {
        "quality":                        "quarantine",
        "pipelines.autoOptimize.managed": "true",
    }
)
def mcc_codes_quarantine():
    return (
        spark.readStream.option("skipChangeCommits", "true").table("mcc_codes_raw")
           .withColumn("failure_reasons", F.concat_ws(", ",
               F.when(F.col("mcc_code").isNull(),
                      F.lit("null_mcc_code")),
               F.when(F.col("description").isNull(),
                      F.lit("null_description")),
               F.when(~F.col("risk_level")
                        .isin(VALID_RISK_LEVELS),
                      F.lit("invalid_risk_level")),
               F.when(~F.col("requires_enhanced_dd")
                        .isin(VALID_ENHANCED_DD),
                      F.lit("invalid_enhanced_dd_flag")),
           ))
           .filter(F.col("failure_reasons") != "")
           .withColumn("quarantine_ts",     F.current_timestamp())
           .withColumn("quarantine_status", F.lit("PENDING_REVIEW"))
    )


@dlt.table(
    name    = "mcc_files_audit",
    comment = "Quality metrics for both reference file streams per run",
    table_properties = {"quality": "audit"}
)
def reference_files_audit():
    mcc_raw        = spark.read.table("mcc_codes_raw")
    mcc_quarantine = spark.read.table("mcc_codes_quarantine")

    counts = (
        mcc_raw.agg(
            F.count("*").alias("mcc_total_rows"),
            F.countDistinct("source_file").alias("mcc_files_processed"),
        )
        .crossJoin(mcc_quarantine.agg(F.count("*").alias("mcc_quarantine_rows")))
    )

    top_failures = mcc_quarantine.groupBy("failure_reasons").agg(
        F.count("*").alias("count")
    ).orderBy(F.desc("count")).limit(5).agg(
        F.to_json(F.collect_list(
            F.struct("failure_reasons", "count")
        )).alias("mcc_top_failures")
    )

    return (
        counts
        .crossJoin(top_failures)
        .withColumn("run_ts", F.current_timestamp())
        .withColumn("mcc_quarantine_pct",
            F.when(F.col("mcc_total_rows") > 0,
                   F.round(F.col("mcc_quarantine_rows") / F.col("mcc_total_rows") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
        .withColumn("audit_status",
            F.when(
                (F.col("mcc_total_rows") > 0) &
                (F.col("mcc_quarantine_pct") <= 5.0),
                F.lit("PASS")
            ).otherwise(F.lit("FAIL"))
        )
        .select(
            "run_ts", "mcc_total_rows", "mcc_quarantine_rows",
            "mcc_quarantine_pct", "mcc_files_processed",
            "mcc_top_failures", "audit_status"
        )
    )

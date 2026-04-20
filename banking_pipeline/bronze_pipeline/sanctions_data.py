import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, BooleanType, ArrayType, DoubleType
)

SANCTIONS_LANDING_PATH = "s3://fraud-demo-bucket-043309328060-us-east-1-an/reference/sanctions/"

VALID_ENTITY_TYPES = [
    "INDIVIDUAL","ORGANISATION","VESSEL","AIRCRAFT"
]
VALID_PROGRAMS = [
    "OFAC_SDN","UN_CONSOLIDATED","EU_CONSOLIDATED",
    "HMT_CONSOLIDATED","OFAC_CONSOLIDATED"
]


@dlt.table(
    name    = "sanctions_raw",
    comment = "Bronze: sanctions and watchlist entities, auto-loaded from S3 JSON",
    table_properties = {
        "quality":                        "bronze",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect("valid_entity_id",   "entity_id IS NOT NULL")
@dlt.expect("valid_full_name",   "full_name IS NOT NULL")
@dlt.expect("valid_entity_type",
    "entity_type IN ('INDIVIDUAL','ORGANISATION','VESSEL','AIRCRAFT')")
@dlt.expect("valid_risk_score",  "risk_score BETWEEN 0.0 AND 1.0")
@dlt.expect("valid_sanctions_program",
    "sanctions_program IN ('OFAC_SDN','UN_CONSOLIDATED',"
    "'EU_CONSOLIDATED','HMT_CONSOLIDATED','OFAC_CONSOLIDATED')")
def sanctions_raw():
    from pyspark.sql.types import ArrayType, DoubleType

    record_schema = StructType([
        StructField("entity_id",          StringType(),           False),
        StructField("entity_type",        StringType(),           True),
        StructField("full_name",          StringType(),           False),
        StructField("aliases",            ArrayType(StringType()),True),
        StructField("country_of_origin",  StringType(),           True),
        StructField("sanctions_program",  StringType(),           True),
        StructField("listed_date",        StringType(),           True),
        StructField("last_updated",       StringType(),           True),
        StructField("is_active",          BooleanType(),          True),
        StructField("risk_score",         DoubleType(),           True),
        StructField("source_list",        StringType(),           True),
    ])

    envelope_schema = StructType([
        StructField("schema_version", StringType(),              True),
        StructField("generated_at",   StringType(),              True),
        StructField("record_count",   StringType(),              True),
        StructField("records",        ArrayType(record_schema),  True),
    ])

    raw_stream = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",        "text")   # read as raw text
             .option("cloudFiles.schemaLocation",
                     "dbfs:/checkpoints/bronze/sanctions/schema")
             .option("wholetext",                "true")   # one row per file
             .load(SANCTIONS_LANDING_PATH)
             .withColumn("source_file",
                 F.col("_metadata.file_path"))
             .withColumn("source_file_modified_ts",
                 F.col("_metadata.file_modification_time"))
    )

    parsed = (
        raw_stream
        .withColumn("envelope",
            F.from_json(F.col("value"), envelope_schema))
        .withColumn("record", F.explode("envelope.records"))
    )

    return parsed.select(
        F.col("record.entity_id").alias("entity_id"),
        F.col("record.entity_type").alias("entity_type"),
        F.col("record.full_name").alias("full_name"),
        F.col("record.aliases").alias("aliases"),
        F.col("record.country_of_origin").alias("country_of_origin"),
        F.col("record.sanctions_program").alias("sanctions_program"),
        F.col("record.listed_date").alias("listed_date"),
        F.col("record.last_updated").alias("last_updated"),
        F.col("record.is_active").alias("is_active"),
        F.col("record.risk_score").alias("risk_score"),
        F.col("record.source_list").alias("source_list"),
        F.col("source_file"),
        F.col("source_file_modified_ts"),
        F.current_timestamp().alias("ingestion_ts"),
        F.to_date(F.current_timestamp()).alias("ingestion_date"),
        F.lit("1.0.0").alias("pipeline_version"),
    )

@dlt.table(
    name    = "sanctions_quarantine",
    comment = "Bronze: sanctions rows failing quality rules",
    table_properties = {
        "quality":                        "quarantine",
        "pipelines.autoOptimize.managed": "true",
    }
)
def sanctions_quarantine():
    return (
        dlt.read_stream("sanctions_raw")
           .withColumn("failure_reasons", F.concat_ws(", ",
               F.when(F.col("entity_id").isNull(),
                      F.lit("null_entity_id")),
               F.when(F.col("full_name").isNull(),
                      F.lit("null_full_name")),
               F.when(~F.col("entity_type")
                        .isin(VALID_ENTITY_TYPES),
                      F.lit("invalid_entity_type")),
               F.when(~F.col("sanctions_program")
                        .isin(VALID_PROGRAMS),
                      F.lit("invalid_sanctions_program")),
               F.when(
                   (F.col("risk_score") < 0) |
                   (F.col("risk_score") > 1),
                   F.lit("risk_score_out_of_range")),
           ))
           .filter(F.col("failure_reasons") != "")
           .withColumn("quarantine_ts",     F.current_timestamp())
           .withColumn("quarantine_status", F.lit("PENDING_REVIEW"))
    )
@dlt.table(
    name    = "sanction_files_audit",
    comment = "Quality metrics for both reference file streams per run",
    table_properties = {"quality": "audit"}
)
def reference_files_audit():
    import json
    from datetime import datetime, timezone

    san_raw        = dlt.read("sanctions_raw")
    san_quarantine = dlt.read("sanctions_quarantine")

    san_total  = san_raw.count()
    san_bad    = san_quarantine.count()
    san_q_rate = round(san_bad / san_total * 100, 4) if san_total > 0 else 0.0

    san_files = (
        san_raw.select("source_file")
               .distinct().count()
    )


    san_failures = (
        san_quarantine
        .groupBy("failure_reasons").count()
        .orderBy(F.desc("count")).limit(5)
        .toPandas().to_dict("records")
    ) if san_bad > 0 else []

    status = (
        "FAIL" if san_total == 0
                  or san_q_rate > 5.0
        else "PASS"
    )

    from pyspark.sql.types import (
        StructType, StructField, LongType,
        DoubleType, StringType, TimestampType
    )
    schema = StructType([
        StructField("sanctions_total_rows",    LongType(),      False),
        StructField("sanctions_quarantine_rows",LongType(),     False),
        StructField("sanctions_quarantine_pct",DoubleType(),    False),
        StructField("sanctions_files_processed",LongType(),     False),
        StructField("sanctions_top_failures",  StringType(),    True),
        StructField("audit_status",            StringType(),    False),
    ])

    return spark.createDataFrame([{
        "run_ts":                   datetime.now(timezone.utc).replace(tzinfo=None),
        "sanctions_total_rows":     san_total,
        "sanctions_quarantine_rows":san_bad,
        "sanctions_quarantine_pct": san_q_rate,
        "sanctions_files_processed":san_files,
        "sanctions_top_failures":   json.dumps(san_failures),
        "audit_status":             status,
    }], schema=schema)

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType
)

VALID_CURRENCIES = ["GBP","USD","EUR","AED","NGN","CNY"]
VALID_TXN_TYPES  = ["PURCHASE","TRANSFER","ATM_WITHDRAWAL",
                    "DIRECT_DEBIT","STANDING_ORDER","REFUND"]
VALID_CHANNELS   = ["mobile_app","online_banking",
                    "branch","atm","pos_terminal"]

@dlt.table(
    name    = "transactions_raw",
    comment = "Bronze: all events landed as-is, warn-only expectations",
    table_properties = {
        "quality":                            "bronze",
        "delta.enableChangeDataFeed":         "true",
        "pipelines.autoOptimize.managed":     "true",
    }
)
@dlt.expect("valid_txn_id",      "txn_id IS NOT NULL")
@dlt.expect("valid_account_id",  "account_id IS NOT NULL")
@dlt.expect("positive_amount",   "amount > 0")
@dlt.expect("valid_event_ts",    "event_ts IS NOT NULL")
@dlt.expect("known_currency",    f"currency IN ({','.join(repr(c) for c in VALID_CURRENCIES)})")
@dlt.expect("known_txn_type",    f"txn_type IN ({','.join(repr(t) for t in VALID_TXN_TYPES)})")
def transactions_raw():
    return (
        spark.readStream
             .format("delta")
             .option("maxFilesPerTrigger", 10)
             .table("banking_demo.bronze.transactions_source")
             .withColumn("ingestion_ts",     F.current_timestamp())
             .withColumn("ingestion_date",   F.to_date(F.current_timestamp()))
             .withColumn("pipeline_version", F.lit("1.0.0"))
             .withColumn("source_system",    F.lit("kafka_simulator"))
    )


@dlt.table(
    name    = "transactions_quarantine",
    comment = "Bronze: rows failing quality rules, pending investigation",
    table_properties = {
        "quality":                        "quarantine",
        "pipelines.autoOptimize.managed": "true",
    }
)
def transactions_quarantine():
    return (
        dlt.read_stream("transactions_raw")
           .withColumn("failure_reasons", F.concat_ws(", ",
               F.when(F.col("txn_id").isNull(),
                      F.lit("null_txn_id")),
               F.when(F.col("account_id").isNull(),
                      F.lit("null_account_id")),
               F.when(F.col("customer_id").isNull(),
                      F.lit("null_customer_id")),
               F.when(F.col("amount") <= 0,
                      F.lit("non_positive_amount")),
               F.when(F.col("event_ts").isNull(),
                      F.lit("null_event_ts")),
               F.when(~F.col("currency").isin(VALID_CURRENCIES),
                      F.lit("invalid_currency")),
               F.when(~F.col("txn_type").isin(VALID_TXN_TYPES),
                      F.lit("invalid_txn_type")),
           ))
           .filter(F.col("failure_reasons") != "")
           .withColumn("quarantine_ts",     F.current_timestamp())
           .withColumn("quarantine_status", F.lit("PENDING_REVIEW"))
    )

@dlt.table(
    name    = "transactions_cleansed_stage",
    comment = "Bronze: quality-passed rows, ready for silver",
    table_properties = {
        "quality":                            "bronze_cleansed",
        "delta.enableChangeDataFeed":         "true",
        "pipelines.autoOptimize.managed":     "true",
    }
)
@dlt.expect_or_fail("cleansed_txn_id",
                    "txn_id IS NOT NULL")
@dlt.expect_or_fail("cleansed_account_id",
                    "account_id IS NOT NULL")
@dlt.expect_or_fail("cleansed_amount",
                    "amount > 0")
@dlt.expect_or_fail("cleansed_event_ts",
                    "event_ts IS NOT NULL")
def transactions_cleansed_stage():
    return (
        dlt.read_stream("transactions_raw")
           .filter(F.col("txn_id").isNotNull())
           .filter(F.col("account_id").isNotNull())
           .filter(F.col("customer_id").isNotNull())
           .filter(F.col("amount") > 0)
           .filter(F.col("event_ts").isNotNull())
           .filter(F.col("currency").isin(VALID_CURRENCIES))
           .filter(F.col("txn_type").isin(VALID_TXN_TYPES))
    )


@dlt.table(
    name    = "transactions_data_audit",
    comment = "Quality metrics snapshot — refreshed each pipeline run",
    table_properties = {"quality": "audit"}
)
def pipeline_audit():
    raw        = dlt.read("transactions_raw")
    cleansed   = dlt.read("transactions_cleansed_stage")
    quarantine = dlt.read("transactions_quarantine")

    raw_count        = raw.agg(F.count("*").alias("total_rows"))
    cleansed_count   = cleansed.agg(F.count("*").alias("good_rows"))
    quarantine_count = quarantine.agg(F.count("*").alias("quarantine_rows"))

    counts = (
        raw_count
        .crossJoin(cleansed_count)
        .crossJoin(quarantine_count)
        .withColumn("quarantine_pct",
            F.when(F.col("total_rows") > 0,
                   F.round(F.col("quarantine_rows").cast("double")
                           / F.col("total_rows").cast("double") * 100, 4))
             .otherwise(F.lit(0.0)))
    )

    currency_dist = (
        raw.groupBy("currency").agg(F.count("*").alias("cnt"))
           .agg(F.to_json(F.map_from_arrays(
               F.collect_list("currency"),
               F.collect_list("cnt")
           )).alias("currency_dist"))
    )

    top_failures = (
        quarantine
        .groupBy("failure_reasons").agg(F.count("*").alias("cnt"))
        .orderBy(F.desc("cnt"))
        .limit(5)
        .agg(F.to_json(F.collect_list(
            F.struct("failure_reasons", "cnt")
        )).alias("top_failure_reasons"))
    )

    return (
        counts
        .crossJoin(currency_dist)
        .crossJoin(top_failures)
        .select(
            F.current_timestamp().alias("run_ts"),
            F.col("total_rows"),
            F.col("good_rows"),
            F.col("quarantine_rows"),
            F.col("quarantine_pct"),
            F.col("currency_dist"),
            F.col("top_failure_reasons"),
            F.when((F.col("total_rows") > 0) & (F.col("quarantine_pct") < 10.0),
                   F.lit("PASS"))
             .otherwise(F.lit("FAIL")).alias("audit_status"),
        )
    )

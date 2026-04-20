import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

STAGING_TABLE  = "banking_demo.bronze.fx_rates_stage"
VALID_QUOTE_CCYS = ["USD","EUR","AED","NGN","CNY","CHF","JPY","SGD"]

FX_RATE_SCHEMA = StructType([
    StructField("rate_id",          StringType(),    False),
    StructField("base_currency",    StringType(),    False),
    StructField("quote_currency",   StringType(),    False),
    StructField("rate",             DoubleType(),    False),
    StructField("source_ts",        StringType(),    True),
    StructField("fetch_ts",         TimestampType(), False),
    StructField("provider",         StringType(),    False),
    StructField("pipeline_version", StringType(),    False),
])

@dlt.table(
    name    = "market_data_raw",
    comment = "Bronze: FX rates read from staging, one row per pair per fetch",
    table_properties = {
        "quality":                        "bronze",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect("valid_rate_id",   "rate_id IS NOT NULL")
@dlt.expect("positive_rate",   "rate > 0")
@dlt.expect("valid_fetch_ts",  "fetch_ts IS NOT NULL")
@dlt.expect("known_quote_ccy",
    "quote_currency IN ('USD','EUR','AED','NGN','CNY','CHF','JPY','SGD')")
def market_data_raw():
    return (
        spark.read
             .format("delta")
             .table(STAGING_TABLE)
    )



@dlt.table(
    name    = "market_data_parsed",
    comment = "Bronze: typed timestamps, staleness flag, deduplicated",
    table_properties = {
        "quality":                        "bronze_parsed",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_or_drop("no_duplicate_rate_id", "rate_id IS NOT NULL")
@dlt.expect_or_fail("parsed_rate_positive", "rate > 0")
def market_data_parsed():
    return (
        dlt.read("market_data_raw")
           .withColumn(
               "source_ts_parsed",
               F.to_timestamp(
                   F.regexp_replace("source_ts", "^\\w+,\\s*", ""),
                   "dd MMM yyyy HH:mm:ss xx"
               )
           )
           .withColumn(
               "rate_date",
               F.to_date("fetch_ts")
           )
           .withColumn(
               "rate_age_minutes",
               (F.unix_timestamp("fetch_ts") -
                F.unix_timestamp("source_ts_parsed")) / 60
           )
           .withColumn(
               "is_stale",
               F.col("rate_age_minutes") > 30
           )
           .dropDuplicates(["rate_id"])
           .drop("source_ts")
           .withColumnRenamed("source_ts_parsed", "source_ts")
    )

@dlt.table(
    name    = "market_data_quarantine",
    comment = "Bronze: FX rate rows failing quality rules",
    table_properties = {
        "quality":                        "quarantine",
        "pipelines.autoOptimize.managed": "true",
    }
)
def market_data_quarantine():
    return (
        dlt.read("market_data_raw")
           .withColumn("failure_reasons", F.concat_ws(", ",
               F.when(F.col("rate_id").isNull(),
                      F.lit("null_rate_id")),
               F.when(F.col("rate") <= 0,
                      F.lit("non_positive_rate")),
               F.when(F.col("fetch_ts").isNull(),
                      F.lit("null_fetch_ts")),
               F.when(F.col("base_currency") != "GBP",
                      F.lit("unexpected_base_currency")),
               F.when(~F.col("quote_currency")
                        .isin(VALID_QUOTE_CCYS),
                      F.lit("unknown_quote_currency")),
               F.when(F.col("rate") > 100000,
                      F.lit("rate_suspiciously_high")),
           ))
           .filter(F.col("failure_reasons") != "")
           .withColumn("quarantine_ts",
                       F.current_timestamp())
           .withColumn("quarantine_status",
                       F.lit("PENDING_REVIEW"))
    )

    
@dlt.table(
    name    = "market_data_audit",
    comment = "Quality metrics for each market data pipeline run",
    table_properties = {"quality": "audit"}
)
def market_data_audit():
    import json
    from datetime import datetime, timezone
    from pyspark.sql.window import Window
    from pyspark.sql.types import (
        StructType, StructField, LongType,
        DoubleType, StringType, TimestampType, BooleanType
    )

    raw        = dlt.read("market_data_raw")
    parsed     = dlt.read("market_data_parsed")
    quarantine = dlt.read("market_data_quarantine")

    # ── single collect for all scalar metrics ─────────────────
    counts = (
        raw.agg(F.count("*").alias("total_rows"))
        .crossJoin(
            parsed.agg(
                F.count("*").alias("good_rows"),
                F.coalesce(
                    F.max("rate_age_minutes"),
                    F.lit(0.0)
                ).alias("max_rate_age_mins"),
                F.sum(
                    F.when(F.col("is_stale") == True, 1)
                     .otherwise(0)
                ).alias("stale_rate_count"),
            )
        )
        .crossJoin(
            quarantine.agg(F.count("*").alias("quarantine_rows"))
        )
        .withColumn(
            "quarantine_pct",
            F.when(
                F.col("total_rows") > 0,
                F.round(
                    F.col("quarantine_rows") / F.col("total_rows") * 100,
                    4
                )
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "provider_stale",
            F.col("max_rate_age_mins") > 30
        )
        .withColumn(
            "audit_status",
            F.when(
                (F.col("total_rows") == 0) |
                (F.col("quarantine_pct") > 5.0) |
                (F.col("stale_rate_count") > 0),
                F.lit("FAIL")
            ).otherwise(F.lit("PASS"))
        )
    )

    row = counts.collect()[0]

    # ── rate snapshot — latest rate per currency pair ──────────
    # toPandas unavoidable here — need a dict keyed by currency
    rate_snapshot = (
        parsed
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("quote_currency")
                      .orderBy(F.desc("fetch_ts"))
            )
        )
        .filter(F.col("rn") == 1)
        .select(
            F.col("quote_currency"),
            F.round("rate", 6).alias("rate")
        )
        .toPandas()
        .set_index("quote_currency")["rate"]
        .to_dict()
    )

    # ── top failure reasons — no pandas equivalent for dict records
    top_failures = (
        quarantine
        .groupBy("failure_reasons")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
        .limit(5)
        .toPandas()
        .to_dict("records")
    ) if row["quarantine_rows"] > 0 else []

    # ── build audit schema ─────────────────────────────────────
    audit_schema = StructType([
        StructField("run_ts",              TimestampType(), False),
        StructField("total_rows",          LongType(),      False),
        StructField("good_rows",           LongType(),      False),
        StructField("quarantine_rows",     LongType(),      False),
        StructField("quarantine_pct",      DoubleType(),    False),
        StructField("max_rate_age_mins",   DoubleType(),    False),
        StructField("stale_rate_count",    LongType(),      False),
        StructField("provider_stale",      BooleanType(),   False),
        StructField("rate_snapshot",       StringType(),    True),
        StructField("top_failure_reasons", StringType(),    True),
        StructField("audit_status",        StringType(),    False),
    ])

    return spark.createDataFrame([{
        "run_ts":              datetime.now(timezone.utc).replace(tzinfo=None),
        "total_rows":          int(row["total_rows"]),
        "good_rows":           int(row["good_rows"]),
        "quarantine_rows":     int(row["quarantine_rows"]),
        "quarantine_pct":      float(row["quarantine_pct"]),
        "max_rate_age_mins":   float(row["max_rate_age_mins"]),
        "stale_rate_count":    int(row["stale_rate_count"]),
        "provider_stale":      bool(row["provider_stale"]),
        "rate_snapshot":       json.dumps(rate_snapshot),
        "top_failure_reasons": json.dumps(top_failures),
        "audit_status":        str(row["audit_status"]),
    }], schema=audit_schema)


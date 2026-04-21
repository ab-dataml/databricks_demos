import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType,
    TimestampType, DateType, LongType
)

BRONZE_SOURCE    = "banking_demo.bronze.market_data_parsed"
VALID_CURRENCIES = ["USD","EUR","AED","NGN","CNY","CHF","JPY","SGD"]

FX_RATES_SILVER_SCHEMA = StructType([
    StructField("rate_id",             StringType(),    False),
    StructField("base_currency",       StringType(),    False),
    StructField("quote_currency",      StringType(),    False),
    StructField("rate",                DoubleType(),    False),
    StructField("source_ts",           TimestampType(), True),
    StructField("fetch_ts",            TimestampType(), False),
    StructField("rate_date",           DateType(),      False),
    StructField("rate_age_minutes",    DoubleType(),    True),
    StructField("prev_rate",           DoubleType(),    True),
    StructField("rate_change_pct",     DoubleType(),    True),
    StructField("daily_avg_rate",      DoubleType(),    True),
    StructField("significant_move",    BooleanType(),   False),
    StructField("provider",            StringType(),    False),
    StructField("silver_processed_ts", TimestampType(), False),
    StructField("pipeline_version",    StringType(),    False),
])

@dlt.table(
    name    = "fx_rates_silver",
    comment = "Silver: normalised FX rates with change metrics, stale rates excluded",
    table_properties = {
        "quality":                        "silver",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_or_drop("silver_valid_rate_id",
                    "rate_id IS NOT NULL")
@dlt.expect_or_drop("silver_positive_rate",
                    "rate > 0")
@dlt.expect_or_drop("silver_valid_fetch_ts",
                    "fetch_ts IS NOT NULL")
@dlt.expect("silver_known_currency",
            "quote_currency IN ('USD','EUR','AED',"
            "'NGN','CNY','CHF','JPY','SGD')")
@dlt.expect("silver_rate_ceiling",
            "rate < 100000")
def fx_rates_silver():
    bronze = spark.read.table(BRONZE_SOURCE)

    pair_window = (
        Window
        .partitionBy("quote_currency")
        .orderBy("fetch_ts")
    )

    daily_window = (
        Window
        .partitionBy("quote_currency", "rate_date")
    )

    enriched = (
        bronze
        .filter(F.col("rate") > 0)
        .filter(F.col("rate_id").isNotNull())
        .withColumn(
            "prev_rate",
            F.lag("rate", 1).over(pair_window)
        )
        .withColumn(
            "rate_change_pct",
            F.when(
                F.col("prev_rate").isNotNull() &
                (F.col("prev_rate") > 0),
                F.round(
                    (F.col("rate") - F.col("prev_rate")) /
                    F.col("prev_rate") * 100,
                    6
                )
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
        .withColumn(
            "significant_move",
            F.when(
                F.abs(F.col("rate_change_pct")) > 1.0,
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn(
            "daily_avg_rate",
            F.round(
                F.avg("rate").over(daily_window),
                6
            )
        )
        .withColumn(
            "silver_processed_ts",
            F.current_timestamp()
        )
        .withColumn(
            "pipeline_version",
            F.lit("1.0.0")
        )
    )

    return enriched.select(
        [f.name for f in FX_RATES_SILVER_SCHEMA.fields]
    )


@dlt.table(
    name    = "fx_rates_pivoted",
    comment = "Silver: FX rates pivoted wide — one row per fetch_ts",
    table_properties = {
        "quality":                        "silver",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_or_fail("pivoted_has_usd", "GBP_USD IS NOT NULL")
@dlt.expect_or_fail("pivoted_has_eur", "GBP_EUR IS NOT NULL")
def fx_rates_pivoted():
    return (
        dlt.read("fx_rates_silver")
           .groupBy("fetch_ts", "rate_date", "base_currency")
           .pivot(
               "quote_currency",
               VALID_CURRENCIES
           )
           .agg(F.first("rate"))
           .select(
               F.col("fetch_ts"),
               F.col("rate_date"),
               F.col("base_currency"),
               F.col("USD").alias("GBP_USD"),
               F.col("EUR").alias("GBP_EUR"),
               F.col("AED").alias("GBP_AED"),
               F.col("NGN").alias("GBP_NGN"),
               F.col("CNY").alias("GBP_CNY"),
               F.col("CHF").alias("GBP_CHF"),
               F.col("JPY").alias("GBP_JPY"),
               F.col("SGD").alias("GBP_SGD"),
               F.current_timestamp().alias("silver_processed_ts"),
               F.lit("1.0.0").alias("pipeline_version"),
           )
    )

@dlt.table(
    name    = "fx_rates_silver_quarantine",
    comment = "Silver: FX rate rows failing silver quality rules",
    table_properties = {
        "quality":                        "quarantine",
        "pipelines.autoOptimize.managed": "true",
    }
)
def fx_rates_silver_quarantine():
    return (
        spark.read.table(BRONZE_SOURCE)
             .withColumn("failure_reasons", F.concat_ws(", ",
                 F.when(F.col("rate_id").isNull(),
                        F.lit("null_rate_id")),
                 F.when(F.col("rate") <= 0,
                        F.lit("non_positive_rate")),
                 F.when(F.col("fetch_ts").isNull(),
                        F.lit("null_fetch_ts")),
                 F.when(F.col("rate") >= 100000,
                        F.lit("rate_exceeds_ceiling")),
                 F.when(
                     ~F.col("quote_currency")
                       .isin(VALID_CURRENCIES),
                     F.lit("unknown_currency")),
             ))
             .filter(F.col("failure_reasons") != "")
             .withColumn("quarantine_layer",  F.lit("silver"))
             .withColumn("quarantine_ts",     F.current_timestamp())
             .withColumn("quarantine_status", F.lit("PENDING_REVIEW"))
    )



@dlt.table(
    name    = "fx_rates_silver_audit",
    comment = "Silver FX rates quality metrics per pipeline run",
    table_properties = {"quality": "audit"}
)
def fx_rates_silver_audit():
    import json
    from datetime import datetime, timezone
    from pyspark.sql.types import (
        StructType, StructField, LongType,
        DoubleType, StringType, TimestampType
    )

    silver     = dlt.read("fx_rates_silver")
    quarantine = dlt.read("fx_rates_silver_quarantine")

    counts = (
        silver.agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("quote_currency").alias("currency_pairs"),
            F.countDistinct("rate_date").alias("distinct_dates"),
            F.sum(
                F.when(F.col("significant_move") == True, 1)
                 .otherwise(0)
            ).alias("significant_move_count"),
            F.coalesce(
                F.round(F.avg("rate_change_pct"), 6),
                F.lit(0.0)
            ).alias("avg_rate_change_pct"),
        )
        .crossJoin(
            quarantine.agg(F.count("*").alias("quarantine_rows"))
        )
        .withColumn(
            "quarantine_pct",
            F.when(
                F.col("total_rows") > 0,
                F.round(
                    F.col("quarantine_rows") /
                    F.col("total_rows") * 100, 4
                )
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "audit_status",
            F.when(
                (F.col("total_rows") == 0) |
                (F.col("quarantine_pct") > 5.0) |
                (F.col("currency_pairs") < F.lit(len(VALID_CURRENCIES))),
                F.lit("FAIL")
            ).otherwise(F.lit("PASS"))
        )
    )

    row = counts.collect()[0]

    top_failures = (
        quarantine
        .groupBy("failure_reasons")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
        .limit(5)
        .toPandas()
        .to_dict("records")
    ) if row["quarantine_rows"] > 0 else []

    audit_schema = StructType([
        StructField("run_ts",                 TimestampType(), False),
        StructField("total_rows",             LongType(),      False),
        StructField("quarantine_rows",        LongType(),      False),
        StructField("quarantine_pct",         DoubleType(),    False),
        StructField("currency_pairs",         LongType(),      False),
        StructField("distinct_dates",         LongType(),      False),
        StructField("significant_move_count", LongType(),      False),
        StructField("avg_rate_change_pct",    DoubleType(),    True),
        StructField("top_failure_reasons",    StringType(),    True),
        StructField("audit_status",           StringType(),    False),
    ])

    return spark.createDataFrame([{
        "run_ts":                 datetime.now(timezone.utc).replace(tzinfo=None),
        "total_rows":             int(row["total_rows"]),
        "quarantine_rows":        int(row["quarantine_rows"]),
        "quarantine_pct":         float(row["quarantine_pct"]),
        "currency_pairs":         int(row["currency_pairs"]),
        "distinct_dates":         int(row["distinct_dates"]),
        "significant_move_count": int(row["significant_move_count"]),
        "avg_rate_change_pct":    float(row["avg_rate_change_pct"]),
        "top_failure_reasons":    json.dumps(top_failures),
        "audit_status":           str(row["audit_status"]),
    }], schema=audit_schema)

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
    TimestampType, DateType, BooleanType
)

SILVER_TXN = "banking_demo.silver.transactions_cleansed"
SILVER_FX  = "banking_demo.silver.fx_rates_silver"

VELOCITY_THRESHOLD  = 10
LARGE_TXN_THRESHOLD = 5000
NIGHT_OWL_THRESHOLD = 3
NIGHT_HOURS         = (0, 5)
CROSS_BORDER_PCT    = 0.50

DAILY_RISK_SCHEMA = StructType([
    StructField("event_date",              DateType(),      False),
    StructField("total_txn_count",         LongType(),      False),
    StructField("total_gbp_volume",        DoubleType(),    False),
    StructField("distinct_customers",      LongType(),      False),
    StructField("distinct_accounts",       LongType(),      False),
    StructField("high_risk_txn_count",     LongType(),      False),
    StructField("high_risk_gbp_volume",    DoubleType(),    False),
    StructField("cross_border_count",      LongType(),      False),
    StructField("cross_border_gbp_volume", DoubleType(),    False),
    StructField("sanctioned_count",        LongType(),      False),
    StructField("late_night_count",        LongType(),      False),
    StructField("refund_count",            LongType(),      False),
    StructField("refund_rate_pct",         DoubleType(),    False),
    StructField("avg_txn_gbp_value",       DoubleType(),    False),
    StructField("max_single_txn_gbp",      DoubleType(),    False),
    StructField("daily_risk_band",         StringType(),    False),
    StructField("gold_processed_ts",       TimestampType(), False),
    StructField("pipeline_version",        StringType(),    False),
])

@dlt.table(
    name    = "daily_risk_summary",
    comment = "Gold: daily transaction risk summary",
    table_properties = {
        "quality":                        "gold",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_or_fail("valid_event_date", "event_date IS NOT NULL")
@dlt.expect("positive_volume",   "total_gbp_volume > 0")
@dlt.expect("valid_risk_band",
    "daily_risk_band IN ('LOW','MEDIUM','HIGH','CRITICAL')")
def daily_risk_summary():
    txn = spark.read.table(SILVER_TXN)

    return (
        txn.groupBy("event_date")
           .agg(
               F.count("*").alias("total_txn_count"),
               F.coalesce(F.round(F.sum("gbp_amount"), 2),
                          F.lit(0.0)).alias("total_gbp_volume"),
               F.countDistinct("customer_id")
                .alias("distinct_customers"),
               F.countDistinct("account_id")
                .alias("distinct_accounts"),
               F.sum(F.when(
                   F.col("is_high_risk_merchant") == True, 1)
                   .otherwise(0)).alias("high_risk_txn_count"),
               F.coalesce(F.round(F.sum(F.when(
                   F.col("is_high_risk_merchant") == True,
                   F.col("gbp_amount"))), 2),
                   F.lit(0.0)).alias("high_risk_gbp_volume"),
               F.sum(F.when(
                   F.col("is_cross_border") == True, 1)
                   .otherwise(0)).alias("cross_border_count"),
               F.coalesce(F.round(F.sum(F.when(
                   F.col("is_cross_border") == True,
                   F.col("gbp_amount"))), 2),
                   F.lit(0.0)).alias("cross_border_gbp_volume"),
               F.sum(F.when(
                   F.col("counterparty_sanctioned") == True, 1)
                   .otherwise(0)).alias("sanctioned_count"),
               F.sum(F.when(
                   F.col("txn_hour").between(
                       NIGHT_HOURS[0], NIGHT_HOURS[1]), 1)
                   .otherwise(0)).alias("late_night_count"),
               F.sum(F.when(
                   F.col("txn_type") == "REFUND", 1)
                   .otherwise(0)).alias("refund_count"),
               F.coalesce(F.round(F.avg("gbp_amount"), 2),
                          F.lit(0.0)).alias("avg_txn_gbp_value"),
               F.coalesce(F.round(F.max("gbp_amount"), 2),
                          F.lit(0.0)).alias("max_single_txn_gbp"),
           )
           .withColumn("refund_rate_pct",
               F.round(F.col("refund_count") /
                       F.col("total_txn_count") * 100, 4))
           .withColumn("cross_border_pct",
               F.col("cross_border_count") /
               F.col("total_txn_count"))
           .withColumn("risk_score",
               F.when(F.col("sanctioned_count") > 0,
                      F.lit(100)).otherwise(F.lit(0)) +
               F.when(F.col("high_risk_txn_count") >
                      F.col("total_txn_count") * 0.10,
                      F.lit(30)).otherwise(F.lit(0)) +
               F.when(F.col("cross_border_pct") > 0.30,
                      F.lit(20)).otherwise(F.lit(0)) +
               F.when(F.col("late_night_count") >
                      F.col("total_txn_count") * 0.05,
                      F.lit(15)).otherwise(F.lit(0)) +
               F.when(F.col("refund_rate_pct") > 5.0,
                      F.lit(10)).otherwise(F.lit(0)) +
               F.when(F.col("max_single_txn_gbp") > 10000,
                      F.lit(10)).otherwise(F.lit(0)))
           .withColumn("daily_risk_band",
               F.when(F.col("risk_score") >= 100,
                      F.lit("CRITICAL"))
                .when(F.col("risk_score") >= 50, F.lit("HIGH"))
                .when(F.col("risk_score") >= 20, F.lit("MEDIUM"))
                .otherwise(F.lit("LOW")))
           .drop("cross_border_pct", "risk_score")
           .withColumn("gold_processed_ts", F.current_timestamp())
           .withColumn("pipeline_version",  F.lit("1.0.0"))
           .select([f.name for f in DAILY_RISK_SCHEMA.fields])
    )

@dlt.table(
    name    = "daily_anomaly_flags",
    comment = "Gold: customer-level anomaly flags per day",
    table_properties = {
        "quality":                        "gold",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_event_date",  "event_date IS NOT NULL")
def daily_anomaly_flags():
    txn = spark.read.table(SILVER_TXN)

    return (
        txn.groupBy("customer_id", "event_date")
           .agg(
               F.count("*").alias("daily_txn_count"),
               F.sum(F.when(
                   F.col("is_cross_border") == True, 1)
                   .otherwise(0)).alias("cross_border_count"),
               F.sum(F.when(
                   F.col("txn_hour").between(
                       NIGHT_HOURS[0], NIGHT_HOURS[1]), 1)
                   .otherwise(0)).alias("night_txn_count"),
               F.coalesce(F.max("gbp_amount"),
                          F.lit(0.0)).alias("max_single_txn_gbp"),
               F.sum(F.when(
                   F.col("counterparty_sanctioned") == True, 1)
                   .otherwise(0)).alias("sanctioned_count"),
               F.countDistinct("country_code")
                .alias("distinct_countries"),
               F.coalesce(F.round(F.sum("gbp_amount"), 2),
                          F.lit(0.0)).alias("total_customer_gbp"),
           )
           .withColumn("velocity_flag",
               F.col("daily_txn_count") > VELOCITY_THRESHOLD)
           .withColumn("large_txn_flag",
               F.col("max_single_txn_gbp") > LARGE_TXN_THRESHOLD)
           .withColumn("night_owl_flag",
               F.col("night_txn_count") > NIGHT_OWL_THRESHOLD)
           .withColumn("cross_border_spike_flag",
               F.col("cross_border_count") /
               F.col("daily_txn_count") > CROSS_BORDER_PCT)
           .withColumn("sanctioned_flag",
               F.col("sanctioned_count") > 0)
           .withColumn("multi_country_flag",
               F.col("distinct_countries") > 3)
           .withColumn("any_flag",
               F.col("velocity_flag") |
               F.col("large_txn_flag") |
               F.col("night_owl_flag") |
               F.col("cross_border_spike_flag") |
               F.col("sanctioned_flag") |
               F.col("multi_country_flag"))
           .withColumn("gold_processed_ts", F.current_timestamp())
           .withColumn("pipeline_version",  F.lit("1.0.0"))
    )


@dlt.table(
    name    = "daily_fx_exposure",
    comment = "Gold: daily FX exposure by currency with rate risk flag",
    table_properties = {
        "quality":                        "gold",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_or_fail("valid_currency",   "currency IS NOT NULL")
@dlt.expect_or_fail("valid_event_date", "event_date IS NOT NULL")
@dlt.expect("positive_exposure",        "total_gbp_exposure >= 0")
def daily_fx_exposure():
    txn = spark.read.table(SILVER_TXN)
    fx  = spark.read.table(SILVER_FX)

    daily_fx_ref = (
        fx.groupBy("quote_currency", "rate_date")
          .agg(
              F.round(F.avg("rate"), 6).alias("daily_avg_rate"),
              F.max(F.col("significant_move").cast("int"))
               .cast(BooleanType())
               .alias("significant_move_on_day"),
              F.round(F.min("rate"), 6).alias("daily_low_rate"),
              F.round(F.max("rate"), 6).alias("daily_high_rate"),
          )
    )

    txn_by_currency = (
        txn.filter(F.col("currency") != "GBP")
           .groupBy("currency", "event_date")
           .agg(
               F.count("*").alias("txn_count"),
               F.coalesce(
                   F.round(F.sum("amount"), 2),
                   F.lit(0.0)).alias("total_original_amount"),
               F.coalesce(
                   F.round(F.sum("gbp_amount"), 2),
                   F.lit(0.0)).alias("total_gbp_exposure"),
               F.coalesce(
                   F.round(F.avg("gbp_amount"), 2),
                   F.lit(0.0)).alias("avg_gbp_per_txn"),
               F.countDistinct("customer_id")
                .alias("distinct_customers"),
           )
    )

    return (
        txn_by_currency.join(
            daily_fx_ref,
            on  = (txn_by_currency["currency"] ==
                   daily_fx_ref["quote_currency"]) &
                  (txn_by_currency["event_date"] ==
                   daily_fx_ref["rate_date"]),
            how = "left"
        )
        .drop(daily_fx_ref["quote_currency"])
        .drop(daily_fx_ref["rate_date"])
        .withColumn("rate_risk_flag",
            F.coalesce(
                F.col("significant_move_on_day"),
                F.lit(False)))
        .withColumn("implied_rate",
            F.when(F.col("total_original_amount") > 0,
                F.round(
                    F.col("total_gbp_exposure") /
                    F.col("total_original_amount"), 6)
            ).otherwise(F.lit(None).cast(DoubleType())))
        .drop("significant_move_on_day")
        .withColumn("gold_processed_ts", F.current_timestamp())
        .withColumn("pipeline_version",  F.lit("1.0.0"))
    )


@dlt.table(
    name    = "daily_risk_audit",
    comment = "Gold: quality metrics per daily risk pipeline run",
    table_properties = {"quality": "audit"}
)
def daily_risk_audit():
    import json
    from datetime import datetime, timezone
    from pyspark.sql.types import (
        StructType, StructField, LongType,
        DoubleType, StringType, TimestampType
    )

    summary   = dlt.read("daily_risk_summary")
    anomalies = dlt.read("daily_anomaly_flags")
    fx_exp    = dlt.read("daily_fx_exposure")

    counts = (
        summary.agg(
            F.count("*").alias("summary_days"),
            F.coalesce(
                F.round(F.sum("total_gbp_volume"), 2),
                F.lit(0.0)).alias("total_pipeline_gbp_volume"),
            F.sum(F.when(
                F.col("daily_risk_band").isin("HIGH","CRITICAL"), 1)
                .otherwise(0)).alias("high_risk_days"),
            F.sum("sanctioned_count")
             .alias("total_sanctioned_hits"),
        )
        .crossJoin(anomalies.agg(
            F.count("*").alias("anomaly_rows"),
            F.sum(F.when(F.col("any_flag") == True, 1)
                  .otherwise(0)).alias("flagged_customers"),
            F.sum(F.when(F.col("sanctioned_flag") == True, 1)
                  .otherwise(0)).alias("sanctioned_customers"),
        ))
        .crossJoin(fx_exp.agg(
            F.count("*").alias("fx_exposure_rows"),
            F.sum(F.when(F.col("rate_risk_flag") == True, 1)
                  .otherwise(0)).alias("fx_risk_flag_count"),
        ))
        .withColumn("audit_status",
            F.when(
                (F.col("summary_days") == 0) |
                (F.col("total_sanctioned_hits") > 0) |
                (F.col("sanctioned_customers") > 0),
                F.lit("FAIL")
            ).otherwise(F.lit("PASS")))
    )

    row = counts.collect()[0]

    risk_band_dist = (
        summary.groupBy("daily_risk_band")
               .agg(F.count("*").alias("count"))
               .orderBy("daily_risk_band")
               .toPandas()
               .set_index("daily_risk_band")["count"]
               .to_dict()
    )

    audit_schema = StructType([
        StructField("run_ts",                    TimestampType(), False),
        StructField("summary_days",              LongType(),      False),
        StructField("total_pipeline_gbp_volume", DoubleType(),    False),
        StructField("high_risk_days",            LongType(),      False),
        StructField("total_sanctioned_hits",     LongType(),      False),
        StructField("anomaly_rows",              LongType(),      False),
        StructField("flagged_customers",         LongType(),      False),
        StructField("sanctioned_customers",      LongType(),      False),
        StructField("fx_exposure_rows",          LongType(),      False),
        StructField("fx_risk_flag_count",        LongType(),      False),
        StructField("risk_band_dist",            StringType(),    True),
        StructField("audit_status",              StringType(),    False),
    ])

    return spark.createDataFrame([{
        "run_ts":                    datetime.now(timezone.utc).replace(tzinfo=None),
        "summary_days":              int(row["summary_days"]),
        "total_pipeline_gbp_volume": float(row["total_pipeline_gbp_volume"]),
        "high_risk_days":            int(row["high_risk_days"] or 0),
        "total_sanctioned_hits":     int(row["total_sanctioned_hits"] or 0),
        "anomaly_rows":              int(row["anomaly_rows"]),
        "flagged_customers":         int(row["flagged_customers"] or 0),
        "sanctioned_customers":      int(row["sanctioned_customers"] or 0),
        "fx_exposure_rows":          int(row["fx_exposure_rows"]),
        "fx_risk_flag_count":        int(row["fx_risk_flag_count"] or 0),
        "risk_band_dist":            json.dumps(risk_band_dist),
        "audit_status":              str(row["audit_status"]),
    }], schema=audit_schema)



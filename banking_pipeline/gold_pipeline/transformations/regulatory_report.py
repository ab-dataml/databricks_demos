import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
    TimestampType, DateType, BooleanType
)

SILVER_TXN     = "banking_demo.silver.transactions_cleansed"
SILVER_FX      = "banking_demo.silver.fx_rates_silver"
GOLD_RISK      = "banking_demo.gold.daily_risk_summary"
GOLD_ANOMALIES = "banking_demo.gold.daily_anomaly_flags"

SAR_THRESHOLD        = 10000   # GBP — mandatory reporting threshold
STRUCTURING_WINDOW   = 3       # days to look back for structuring
STRUCTURING_TOTAL    = 9000    # GBP — structuring detection threshold
LARGE_EXPOSURE_PCT   = 0.10    # Basel III large exposure threshold
ASSUMED_CAPITAL_BASE = 5000000 # GBP — demo capital base

@dlt.table(
    name    = "aml_transaction_report",
    comment = "Gold: AML flagged transactions for regulatory reporting",
    table_properties = {
        "quality":                        "gold_regulatory",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_fail("valid_txn_id",
                    "txn_id IS NOT NULL")
@dlt.expect_or_fail("valid_regulatory_status",
                    "regulatory_status IN "
                    "('CLEAR','REVIEW','REPORT')")
@dlt.expect("valid_report_date",
            "report_date IS NOT NULL")
def aml_transaction_report():
    txn = spark.read.table(SILVER_TXN)

    customer_window = (
        Window
        .partitionBy("customer_id")
        .orderBy(F.col("event_ts").cast("long"))
        .rangeBetween(
            -STRUCTURING_WINDOW * 86400,
            0
        )
    )

    flagged = (
        txn
        .withColumn(
            "threshold_breach",
            F.col("gbp_amount") >= SAR_THRESHOLD
        )
        .withColumn(
            "sanctions_hit",
            F.col("counterparty_sanctioned") == True
        )
        .withColumn(
            "rolling_3d_gbp",
            F.sum("gbp_amount").over(customer_window)
        )
        .withColumn(
            "structuring_flag",
            (F.col("rolling_3d_gbp") >= STRUCTURING_TOTAL) &
            (F.col("gbp_amount") < SAR_THRESHOLD)
        )
        .withColumn(
            "high_risk_channel",
            F.col("channel").isin(
                "atm", "online_banking") &
            F.col("is_international") &
            (F.col("gbp_amount") > 3000)
        )
        .withColumn(
            "sar_candidate",
            F.col("threshold_breach") |
            F.col("sanctions_hit") |
            F.col("structuring_flag")
        )
        .withColumn(
            "regulatory_status",
            F.when(
                F.col("sanctions_hit"),
                F.lit("REPORT")
            ).when(
                F.col("threshold_breach") |
                F.col("structuring_flag"),
                F.lit("REVIEW")
            ).otherwise(F.lit("CLEAR"))
        )
        .withColumn(
            "report_reference",
            F.concat_ws("-",
                F.lit("AML"),
                F.date_format("event_date", "yyyyMMdd"),
                F.col("txn_id")
            )
        )
        .withColumn(
            "report_date",
            F.to_date(F.current_timestamp())
        )
        .withColumn(
            "report_generated_ts",
            F.current_timestamp()
        )
        .withColumn(
            "pipeline_version",
            F.lit("1.0.0")
        )
        .withColumn(
            "report_period_start",
            F.date_sub(F.to_date(F.current_timestamp()), 1)
        )
        .withColumn(
            "report_period_end",
            F.to_date(F.current_timestamp())
        )
    )

    return flagged.select(
        "txn_id",
        "account_id",
        "customer_id",
        "counterparty_id",
        "txn_type",
        "channel",
        "amount",
        "currency",
        "gbp_amount",
        "country_code",
        "is_international",
        "merchant_mcc",
        "event_ts",
        "event_date",
        "threshold_breach",
        "sanctions_hit",
        "structuring_flag",
        "high_risk_channel",
        "sar_candidate",
        "rolling_3d_gbp",
        "regulatory_status",
        "report_reference",
        "report_date",
        "report_period_start",
        "report_period_end",
        "report_generated_ts",
        "pipeline_version",
    )

@dlt.table(
    name    = "basel3_exposure_report",
    comment = "Gold: Basel III risk-weighted exposure per customer per day",
    table_properties = {
        "quality":                        "gold_regulatory",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_fail("valid_customer_id",
                    "customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_report_date",
                    "report_date IS NOT NULL")
@dlt.expect("positive_exposure",
            "total_gbp_exposure >= 0")
def basel3_exposure_report():
    txn = spark.read.table(SILVER_TXN)

    RWA_WEIGHTS = F.create_map(
        F.lit("PURCHASE"),         F.lit(1.00),
        F.lit("TRANSFER"),         F.lit(0.75),
        F.lit("ATM_WITHDRAWAL"),   F.lit(0.50),
        F.lit("DIRECT_DEBIT"),     F.lit(0.75),
        F.lit("STANDING_ORDER"),   F.lit(0.75),
        F.lit("REFUND"),           F.lit(0.00),
    )

    return (
        txn.groupBy("customer_id", "event_date")
           .agg(
               F.coalesce(
                   F.round(F.sum("gbp_amount"), 2),
                   F.lit(0.0)
               ).alias("total_gbp_exposure"),
               F.count("*").alias("txn_count"),
               F.coalesce(
                   F.round(F.sum(
                       F.col("gbp_amount") *
                       RWA_WEIGHTS[F.col("txn_type")]
                   ), 2),
                   F.lit(0.0)
               ).alias("risk_weighted_assets"),
               F.coalesce(
                   F.round(F.max("gbp_amount"), 2),
                   F.lit(0.0)
               ).alias("largest_single_txn"),
               F.countDistinct("currency")
                .alias("currency_count"),
               F.countDistinct("country_code")
                .alias("country_count"),
               F.sum(
                   F.when(F.col("is_international") == True, 1)
                    .otherwise(0)
               ).alias("international_txn_count"),
               F.sum(
                   F.when(
                       F.col("counterparty_sanctioned") == True, 1)
                    .otherwise(0)
               ).alias("sanctions_exposure_count"),
           )
           .withColumn(
               "exposure_pct_of_capital",
               F.round(
                   F.col("total_gbp_exposure") /
                   F.lit(ASSUMED_CAPITAL_BASE) * 100,
                   4
               )
           )
           .withColumn(
               "large_exposure_flag",
               F.col("exposure_pct_of_capital") >
               F.lit(LARGE_EXPOSURE_PCT * 100)
           )
           .withColumn(
               "capital_adequacy_band",
               F.when(
                   F.col("sanctions_exposure_count") > 0,
                   F.lit("BREACH")
               ).when(
                   F.col("exposure_pct_of_capital") >= 10.0,
                   F.lit("BREACH")
               ).when(
                   F.col("exposure_pct_of_capital") >= 7.5,
                   F.lit("WATCH")
               ).otherwise(F.lit("ADEQUATE"))
           )
           .withColumn(
               "report_date",
               F.to_date(F.current_timestamp())
           )
           .withColumn(
               "report_generated_ts",
               F.current_timestamp()
           )
           .withColumn("pipeline_version", F.lit("1.0.0"))
    )

    
@dlt.table(
    name    = "regulatory_audit_log",
    comment = "Gold: immutable record of every regulatory pipeline run",
    table_properties = {
        "quality":            "gold_regulatory_audit"
    }
)
def regulatory_audit_log():
    import hashlib
    from datetime import datetime, timezone
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, LongType,
        TimestampType, DateType
    )

    aml      = dlt.read("aml_transaction_report")
    basel    = dlt.read("basel3_exposure_report")

    aml_total     = aml.count()
    aml_report    = aml.filter(
        F.col("regulatory_status") == "REPORT").count()
    aml_review    = aml.filter(
        F.col("regulatory_status") == "REVIEW").count()
    basel_total   = basel.count()
    basel_breach  = basel.filter(
        F.col("capital_adequacy_band") == "BREACH").count()
    basel_watch   = basel.filter(
        F.col("capital_adequacy_band") == "WATCH").count()

    report_period = aml.agg(
        F.min("event_date").alias("start"),
        F.max("event_date").alias("end")
    ).collect()[0]

    content_str = (
        f"aml_total={aml_total}"
        f"|aml_report={aml_report}"
        f"|aml_review={aml_review}"
        f"|basel_total={basel_total}"
        f"|basel_breach={basel_breach}"
        f"|period_start={report_period['start']}"
        f"|period_end={report_period['end']}"
    )
    report_hash = hashlib.sha256(
        content_str.encode()
    ).hexdigest()

    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    schema = StructType([
        StructField("run_id",              StringType(),    False),
        StructField("run_ts",              TimestampType(), False),
        StructField("report_date",         DateType(),      False),
        StructField("period_start",        DateType(),      True),
        StructField("period_end",          DateType(),      True),
        StructField("aml_total_rows",      LongType(),      False),
        StructField("aml_report_count",    LongType(),      False),
        StructField("aml_review_count",    LongType(),      False),
        StructField("basel_total_rows",    LongType(),      False),
        StructField("basel_breach_count",  LongType(),      False),
        StructField("basel_watch_count",   LongType(),      False),
        StructField("report_hash",         StringType(),    False),
        StructField("pipeline_version",    StringType(),    False),
        StructField("status",              StringType(),    False),
    ])

    status = (
        "BREACH_DETECTED" if basel_breach > 0
        else "SAR_REQUIRED" if aml_report > 0
        else "REVIEW_REQUIRED" if aml_review > 0
        else "CLEAR"
    )

    return spark.createDataFrame([{
        "run_id":             report_hash[:16],
        "run_ts":             run_ts,
        "report_date":        run_ts.date(),
        "period_start":       report_period["start"],
        "period_end":         report_period["end"],
        "aml_total_rows":     aml_total,
        "aml_report_count":   aml_report,
        "aml_review_count":   aml_review,
        "basel_total_rows":   basel_total,
        "basel_breach_count": basel_breach,
        "basel_watch_count":  basel_watch,
        "report_hash":        report_hash,
        "pipeline_version":   "1.0.0",
        "status":             status,
    }], schema=schema)


@dlt.table(
    name    = "regulatory_pipeline_audit",
    comment = "Gold: regulatory pipeline quality metrics per run",
    table_properties = {"quality": "audit"}
)
def regulatory_pipeline_audit():
    import json
    from datetime import datetime, timezone
    from pyspark.sql.types import (
        StructType, StructField, LongType,
        StringType, TimestampType
    )

    aml   = dlt.read("aml_transaction_report")
    basel = dlt.read("basel3_exposure_report")
    log   = dlt.read("regulatory_audit_log")

    counts = (
        aml.agg(
            F.count("*").alias("aml_rows"),
            F.sum(F.when(
                F.col("regulatory_status") == "REPORT", 1)
                .otherwise(0)).alias("sar_count"),
            F.sum(F.when(
                F.col("regulatory_status") == "REVIEW", 1)
                .otherwise(0)).alias("review_count"),
            F.sum(F.when(
                F.col("sanctions_hit") == True, 1)
                .otherwise(0)).alias("sanctions_count"),
        )
        .crossJoin(basel.agg(
            F.count("*").alias("basel_rows"),
            F.sum(F.when(
                F.col("capital_adequacy_band") == "BREACH", 1)
                .otherwise(0)).alias("breach_count"),
            F.sum(F.when(
                F.col("capital_adequacy_band") == "WATCH", 1)
                .otherwise(0)).alias("watch_count"),
        ))
        .withColumn("audit_status",
            F.when(
                (F.col("aml_rows") == 0) |
                (F.col("basel_rows") == 0) |
                (F.col("sanctions_count") > 0) |
                (F.col("breach_count") > 0),
                F.lit("FAIL")
            ).otherwise(F.lit("PASS")))
    )

    row = counts.collect()[0]

    latest_log_rows = (
        log.orderBy(F.desc("run_ts"))
           .limit(1)
           .select("run_id", "report_hash", "status")
           .collect()
    )
    latest_log = (
        latest_log_rows[0] if latest_log_rows
        else {"run_id": "N/A", "report_hash": "N/A", "status": "N/A"}
    )

    schema = StructType([
        StructField("run_ts",         TimestampType(), False),
        StructField("aml_rows",       LongType(),      False),
        StructField("sar_count",      LongType(),      False),
        StructField("review_count",   LongType(),      False),
        StructField("sanctions_count",LongType(),      False),
        StructField("basel_rows",     LongType(),      False),
        StructField("breach_count",   LongType(),      False),
        StructField("watch_count",    LongType(),      False),
        StructField("run_id",         StringType(),    False),
        StructField("report_hash",    StringType(),    False),
        StructField("log_status",     StringType(),    False),
        StructField("audit_status",   StringType(),    False),
    ])

    return spark.createDataFrame([{
        "run_ts":          datetime.now(timezone.utc).replace(tzinfo=None),
        "aml_rows":        int(row["aml_rows"]),
        "sar_count":       int(row["sar_count"] or 0),
        "review_count":    int(row["review_count"] or 0),
        "sanctions_count": int(row["sanctions_count"] or 0),
        "basel_rows":      int(row["basel_rows"]),
        "breach_count":    int(row["breach_count"] or 0),
        "watch_count":     int(row["watch_count"] or 0),
        "run_id":          str(latest_log["run_id"]),
        "report_hash":     str(latest_log["report_hash"]),
        "log_status":      str(latest_log["status"]),
        "audit_status":    str(row["audit_status"]),
    }], schema=schema)

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    TimestampType, BooleanType, DateType
)

BRONZE_SOURCE   = "fraud_demo.bronze.transactions_cleansed_stage"
MCC_TABLE       = "fraud_demo.bronze.mcc_codes_raw"
SANCTIONS_TABLE = "fraud_demo.bronze.sanctions_raw"

VALID_CURRENCIES = ["GBP","USD","EUR","AED","NGN","CNY","CHF","JPY","SGD"]
VALID_TXN_TYPES  = ["PURCHASE","TRANSFER","ATM_WITHDRAWAL",
                    "DIRECT_DEBIT","STANDING_ORDER","REFUND"]
VALID_CHANNELS   = ["mobile_app","online_banking",
                    "branch","atm","pos_terminal"]
HIGH_RISK_MCCS   = ["9999","6051","7995","5933","5944"]

SILVER_SCHEMA = StructType([
    StructField("txn_id",              StringType(),    False),
    StructField("account_id",          StringType(),    False),
    StructField("customer_id",         StringType(),    False),
    StructField("counterparty_id",     StringType(),    True),
    StructField("txn_type",            StringType(),    False),
    StructField("channel",             StringType(),    False),
    StructField("amount",              DoubleType(),    False),
    StructField("currency",            StringType(),    False),
    StructField("gbp_amount",          DoubleType(),    True),
    StructField("merchant_name",       StringType(),    True),
    StructField("merchant_mcc",        StringType(),    True),
    StructField("merchant_risk_level", StringType(),    True),
    StructField("country_code",        StringType(),    False),
    StructField("is_international",    BooleanType(),   False),
    StructField("is_cross_border",     BooleanType(),   False),
    StructField("is_high_risk_merchant",BooleanType(),  False),
    StructField("counterparty_sanctioned", BooleanType(), False),
    StructField("device_id",           StringType(),    True),
    StructField("ip_address",          StringType(),    True),
    StructField("event_ts",            TimestampType(), False),
    StructField("event_date",          DateType(),      False),
    StructField("txn_hour",            IntegerType(),   False),
    StructField("txn_day_of_week",     IntegerType(),   False),
    StructField("silver_processed_ts", TimestampType(), False),
    StructField("pipeline_version",    StringType(),    False),
])


@dlt.table(
    name    = "transactions_cleansed",
    comment = "Silver: deduplicated, typed, enriched transactions",
    table_properties = {
        "quality":                        "silver",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_or_drop("silver_valid_txn_id",
                    "txn_id IS NOT NULL")
@dlt.expect_or_drop("silver_valid_account",
                    "account_id IS NOT NULL")
@dlt.expect_or_drop("silver_positive_amount",
                    "amount > 0")
@dlt.expect_or_drop("silver_valid_event_ts",
                    "event_ts IS NOT NULL")
@dlt.expect_or_drop("silver_event_not_future",
                    "event_ts <= current_timestamp()")
@dlt.expect_or_drop("silver_valid_currency",
                    "currency IN ('GBP','USD','EUR','AED',"
                    "'NGN','CNY','CHF','JPY','SGD')")
@dlt.expect("silver_valid_txn_type",
            "txn_type IN ('PURCHASE','TRANSFER','ATM_WITHDRAWAL',"
            "'DIRECT_DEBIT','STANDING_ORDER','REFUND')")
@dlt.expect("silver_valid_channel",
            "channel IN ('mobile_app','online_banking',"
            "'branch','atm','pos_terminal')")
@dlt.expect("silver_amount_ceiling",
            "amount < 1000000")
def transactions_cleansed():
    bronze = (
        spark.readStream
             .format("delta")
             .table(BRONZE_SOURCE)
    )

    mcc_ref = (
        spark.read.table(MCC_TABLE)
             .select(
                 F.col("mcc_code"),
                 F.col("risk_level").alias("merchant_risk_level"),
             )
             .dropDuplicates(["mcc_code"])
    )

    sanctions_ref = (
        spark.read.table(SANCTIONS_TABLE)
             .filter(F.col("is_active") == True)
             .select(
                 F.col("entity_id").alias("counterparty_id"),
                 F.lit(True).alias("is_sanctioned"),
             )
             .dropDuplicates(["counterparty_id"])
    )

    deduped = (
        bronze
        .withWatermark("event_ts", "1 hour")
        .dropDuplicates(["txn_id", "event_ts"])
    )

    fx_rates = {
        "GBP": 1.0000,
        "USD": 0.7840,
        "EUR": 0.8590,
        "AED": 0.2134,
        "NGN": 0.0005,
        "CNY": 0.1082,
        "CHF": 0.8820,
        "JPY": 0.0052,
        "SGD": 0.5810,
    }

    fx_expr = F.create_map(
        *[v for pair in
          [(F.lit(k), F.lit(v)) for k, v in fx_rates.items()]
          for v in pair]
    )

    enriched = (
        deduped
        .withColumn("event_ts",
            F.col("event_ts").cast(TimestampType()))
        .withColumn("amount",
            F.col("amount").cast(DoubleType()))
        .withColumn("is_international",
            F.col("is_international").cast(BooleanType()))
        .withColumn("gbp_amount",
            F.round(
                F.col("amount") * fx_expr[F.col("currency")],
                2
            ))
        .withColumn("event_date",
            F.to_date("event_ts"))
        .withColumn("txn_hour",
            F.hour("event_ts").cast(IntegerType()))
        .withColumn("txn_day_of_week",
            F.dayofweek("event_ts").cast(IntegerType()))
        .withColumn("is_cross_border",
            F.col("country_code") != "GB")
        .withColumn("is_high_risk_merchant",
            F.col("merchant_mcc").isin(HIGH_RISK_MCCS))
        .withColumn("merchant_name",
            F.coalesce(F.col("merchant_name"), F.lit("UNKNOWN")))
        .withColumn("merchant_mcc",
            F.coalesce(F.col("merchant_mcc"), F.lit("0000")))
        .withColumn("silver_processed_ts",
            F.current_timestamp())
        .withColumn("pipeline_version",
            F.lit("1.0.0"))
    )

    with_mcc = enriched.join(
        mcc_ref,
        on  = "merchant_mcc",
        how = "left"
    )

    with_sanctions = with_mcc.join(
        sanctions_ref,
        on  = "counterparty_id",
        how = "left"
    ).withColumn(
        "counterparty_sanctioned",
        F.coalesce(F.col("is_sanctioned"), F.lit(False))
    ).drop("is_sanctioned")

    return with_sanctions.select([
        f.name for f in SILVER_SCHEMA.fields
    ])

@dlt.view(
    name    = "transactions_high_risk",
    comment = "Silver view: transactions requiring enhanced due diligence"
)
def transactions_high_risk():
    return (
        dlt.read("transactions_cleansed")
           .filter(
               (F.col("counterparty_sanctioned") == True) |
               (F.col("is_high_risk_merchant")   == True) |
               (F.col("is_cross_border")          == True) &
               (F.col("gbp_amount")               > 5000) |
               (F.col("txn_hour").between(0, 5))  &
               (F.col("gbp_amount")               > 1000)
           )
    )


@dlt.table(
    name    = "transactions_silver_quarantine",
    comment = "Silver: rows failing silver quality rules",
    table_properties = {
        "quality":                        "quarantine",
        "pipelines.autoOptimize.managed": "true",
    }
)
def transactions_silver_quarantine():
    return (
        spark.readStream
             .format("delta")
             .table(BRONZE_SOURCE)
             .withColumn("failure_reasons", F.concat_ws(", ",
                 F.when(F.col("txn_id").isNull(),
                        F.lit("null_txn_id")),
                 F.when(F.col("account_id").isNull(),
                        F.lit("null_account_id")),
                 F.when(F.col("amount") <= 0,
                        F.lit("non_positive_amount")),
                 F.when(F.col("amount") >= 1000000,
                        F.lit("amount_exceeds_ceiling")),
                 F.when(F.col("event_ts").isNull(),
                        F.lit("null_event_ts")),
                 F.when(F.col("event_ts") > F.current_timestamp(),
                        F.lit("event_ts_in_future")),
                 F.when(~F.col("currency").isin(VALID_CURRENCIES),
                        F.lit("invalid_currency")),
                 F.when(~F.col("txn_type").isin(VALID_TXN_TYPES),
                        F.lit("invalid_txn_type")),
                 F.when(~F.col("channel").isin(VALID_CHANNELS),
                        F.lit("invalid_channel")),
             ))
             .filter(F.col("failure_reasons") != "")
             .withColumn("quarantine_layer",  F.lit("silver"))
             .withColumn("quarantine_ts",     F.current_timestamp())
             .withColumn("quarantine_status", F.lit("PENDING_REVIEW"))
    )    


@dlt.table(
    name    = "transactions_silver_audit",
    comment = "Silver quality metrics per pipeline run",
    table_properties = {"quality": "audit"}
)
def transactions_silver_audit():
    import json
    from datetime import datetime, timezone

    silver     = dlt.read("transactions_cleansed")
    quarantine = dlt.read("transactions_silver_quarantine")

    total      = silver.count()
    bad        = quarantine.count()
    q_rate     = round(bad / total * 100, 4) if total > 0 else 0.0

    sanctioned_count = silver.filter(
        F.col("counterparty_sanctioned") == True).count()

    high_risk_count = silver.filter(
        F.col("is_high_risk_merchant") == True).count()

    cross_border_count = silver.filter(
        F.col("is_cross_border") == True).count()

    total_gbp_volume = silver.agg(
        F.round(F.sum("gbp_amount"), 2)
    ).collect()[0][0] or 0.0

    channel_dist = (
        silver.groupBy("channel").count()
              .toPandas()
              .set_index("channel")["count"]
              .to_dict()
    )

    top_failures = (
        quarantine.groupBy("failure_reasons").count()
        .orderBy(F.desc("count")).limit(5)
        .toPandas().to_dict("records")
    ) if bad > 0 else []

    status = (
        "FAIL" if total == 0
                  or q_rate > 5.0
                  or sanctioned_count > 0
        else "PASS"
    )

    from pyspark.sql.types import (
        StructType, StructField, LongType,
        DoubleType, StringType, TimestampType
    )
    schema = StructType([
        StructField("run_ts",              TimestampType(), False),
        StructField("total_rows",          LongType(),      False),
        StructField("quarantine_rows",     LongType(),      False),
        StructField("quarantine_pct",      DoubleType(),    False),
        StructField("sanctioned_count",    LongType(),      False),
        StructField("high_risk_count",     LongType(),      False),
        StructField("cross_border_count",  LongType(),      False),
        StructField("total_gbp_volume",    DoubleType(),    False),
        StructField("channel_dist",        StringType(),    True),
        StructField("top_failure_reasons", StringType(),    True),
        StructField("audit_status",        StringType(),    False),
    ])

    return spark.createDataFrame([{
        "run_ts":              datetime.now(timezone.utc).replace(tzinfo=None),
        "total_rows":          total,
        "quarantine_rows":     bad,
        "quarantine_pct":      q_rate,
        "sanctioned_count":    sanctioned_count,
        "high_risk_count":     high_risk_count,
        "cross_border_count":  cross_border_count,
        "total_gbp_volume":    float(total_gbp_volume),
        "channel_dist":        json.dumps(channel_dist),
        "top_failure_reasons": json.dumps(top_failures),
        "audit_status":        status,
    }], schema=schema)
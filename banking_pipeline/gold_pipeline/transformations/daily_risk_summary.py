import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType,
    TimestampType, DateType, LongType
)

SILVER_SOURCE = "banking_demo.silver.transactions_cleansed"

@dlt.table(
    name    = "daily_risk_summary",
    comment = "Gold: Daily risk summary",
    table_properties = {
        "quality":                        "gold",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
def fx_rates_silver():
    
    silver = spark.read.table(SILVER_SOURCE)

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


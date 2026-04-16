from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import functions as F

fe = FeatureEngineeringClient()

features_df = (spark.table("fraud_demo.transactions.raw")
    .groupBy("user_id")
    .agg(
        F.count("txn_id").alias("txn_count_7d"),
        F.avg("amount").alias("avg_amount_7d"),
        F.max("amount").alias("max_amount_7d"),
        F.stddev("amount").alias("std_amount_7d"),
        F.sum(F.col("merchant")=="Unknown"
              .cast("int")).alias("unknown_merchant_count")
    ))

fe.create_table(
    name="fraud_demo.transactions.user_features",
    primary_keys=["user_id"],
    df=features_df,
    description="User-level aggregated fraud features"
)

fe.write_table(
    name="fraud_demo.transactions.user_features",
    df=features_df,
    mode="overwrite"
)
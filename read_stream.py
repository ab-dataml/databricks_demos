stream_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "delta")
    .load("fraud_demo.transactions.raw"))

query = (stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/fraud_checkpoint")
    .outputMode("append")
    .toTable("fraud_demo.transactions.streaming_raw"))

query.awaitTermination(30)
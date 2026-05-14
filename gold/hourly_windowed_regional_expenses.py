from pyspark.sql import functions as F

spark.conf.set("spark.sql.shuffle.partitions", 50)

try:

    df = (spark.readStream.format("delta")
        .table("sales_project_streaming.slv.expenses"))

    df = (df.withWatermark("event_time", "30 minutes").groupBy(F.window("event_time", "1 hour"), F.col("region_id"))
        .agg(F.sum("expense_amount").alias("total_spent"), F.avg("expense_amount").alias("avg_spent_per_event"),
            F.approx_count_distinct(F.col("employee_id")).alias("uniq_employees"))
        .withColumn("amt_spent_per_emp", F.round(F.col("total_spent")/F.col("uniq_employees"), 2))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("hour", F.hour(F.col("window_start")))
        .select("window_start", "window_end", "hour", "region_id", "total_spent", "avg_spent_per_event", "uniq_employees",
                "amt_spent_per_emp"))

    reg_df = spark.read.table("sales_project_streaming.slv.regions")

    joined_df = (df.alias("h").join(F.broadcast(reg_df).alias("r"), F.col("h.region_id")==F.col("r.region_id"), "inner")
                .select("h.window_start", "h.window_end", "h.hour", F.col("r.region_name").alias("city"), "h.total_spent",
                        "h.avg_spent_per_event", "h.uniq_employees", "h.amt_spent_per_emp")
                )

    query = (joined_df.writeStream.format("delta")
            .option("checkpointLocation", "/Volumes/sales_project_streaming/gld/checkpoints_vol/hrly_chck_v2_exp_reg/")
            .outputMode("append")
            .trigger(availableNow = True)
            .table("sales_project_streaming.gld.hourly_regional_expenses_agg"))

    if not query.awaitTermination(200):
        query.stop()

except Exception as e:
    print(f"Error Finishing Stream: {e}")
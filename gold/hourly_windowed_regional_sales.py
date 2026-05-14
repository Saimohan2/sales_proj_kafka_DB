from pyspark.sql import functions as F

spark.conf.set("spark.sql.shuffle.partitions", 50)

df = (spark.readStream.format("delta")
      .table("sales_project_streaming.slv.sales"))

df = (df.withWatermark("event_time", "30 minutes")
        .groupBy(F.window("event_time", "1 hour"), F.col("region_id"))
        .agg(F.sum(F.col("amount")).alias("total_revenue"), F.sum(F.col("quantity")).alias("total_units_sold"),
             F.approx_count_distinct(F.col("product_id")).alias("uniq_prods_sold"),
             F.count(F.col("sales_id")).alias("total_orders"), F.max(F.col("amount")).alias("max_order_val"),
             F.min(F.col("amount")).alias("min_order_val"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("hour", F.hour(F.col("window_start")))
        .withColumn("aov", F.round(F.col("total_revenue")/F.col("total_orders"), 2))
        .withColumn("avg_units_per_prod", F.round(F.col("total_units_sold")/F.col("uniq_prods_sold"), 2))
        .select("window_start", "window_end", "hour", "region_id", "total_revenue", "total_units_sold", "uniq_prods_sold", "total_orders",
               "max_order_val", "min_order_val", "aov", "avg_units_per_prod")
        )
reg_df = spark.read.table("sales_project_streaming.slv.regions")

joined_df = (df.alias("h").join(F.broadcast(reg_df).alias("r"), F.col("h.region_id")==F.col("r.region_id"), "inner")
             .select("h.window_start", "h.window_end", "h.hour", F.col("r.region_name").alias("city"),
                     "h.total_revenue", "h.total_units_sold", "h.uniq_prods_sold", "h.total_orders",
                     "h.max_order_val", "h.min_order_val", "h.aov", "h.avg_units_per_prod"))

query = (joined_df.writeStream.format("delta")
            .option("checkpointLocation", "/Volumes/sales_project_streaming/gld/checkpoints_vol/hrly_reg_sales_chck_fin/")
            .outputMode("append")
            .trigger(availableNow = True)
            .table("sales_project_streaming.gld.hourly_regional_sales_agg"))

if not query.awaitTermination(200):
    query.stop()
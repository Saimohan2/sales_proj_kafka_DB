from pyspark.sql import functions as F

df = spark.readStream.format("delta").table("sales_project_streaming.slv.sales")

ten_mins_agg = (df.withWatermark("event_time", "10 minutes")
                .groupBy(F.window("event_time", "10 minutes"), F.col("region_id"))
                .agg(F.sum(F.col("amount")).alias("total_sales_amount"), F.approx_count_distinct(F.col("product_id")).alias("unique_products_sold"),
                          F.sum(F.col("quantity")).alias("total_quantity"), F.count("*").alias("total_orders"))
                     .withColumn("aov", F.round(F.col("total_sales_amount")/F.col("total_orders"),2))
                     )

ten_min_query = (ten_mins_agg.writeStream.format("delta")
         .option("checkpointLocation", "/Volumes/sales_project_streaming/gld/checkpoints_vol/ten_min_agg_chck/")
         .outputMode("append")
         .trigger(availableNow = True)
         .table("sales_project_streaming.gld.ten_min_sales_agg")
         )

if not ten_min_query.awaitTermination(200):
    ten_min_query.stop()
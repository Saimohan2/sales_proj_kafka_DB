from pyspark.sql import functions as F

df = spark.readStream.format("delta").table("sales_project_streaming.slv.sales")

hourly_sales_agg = (df.withWatermark("event_time", "10 minutes")
                     .groupBy(F.window("event_time", "1 hour"), F.col("region_id"))
                     .agg(F.sum(F.col("amount")).alias("total_sales_amount"), F.approx_count_distinct(F.col("product_id")).alias("unique_products_sold"),
                          F.sum(F.col("quantity")).alias("total_quantity"), F.count("*").alias("total_orders"))
                     .withColumn("aov", F.round(F.col("total_sales_amount")/F.col("total_orders"),2))
                     )

hourly_query = (hourly_sales_agg.writeStream.format("delta")
         .option("checkpointLocation", "/Volumes/sales_project_streaming/gld/checkpoints_vol/hourly_sales_chck/")
         .outputMode("append")
         .trigger(availableNow = True)
         .table("sales_project_streaming.gld.hourly_sales_agg")
         )

if not hourly_query.awaitTermination(200):
    hourly_query.stop()
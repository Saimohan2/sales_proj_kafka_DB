from pyspark.sql import functions as F

spark.conf.set("spark.sql.shuffle.partitions", 50)

try:

    df = (spark.readStream.format("delta")
        .table("sales_project_streaming.slv.sales"))

    daily_df = (df.withWatermark("event_time", "1 day")
                    .groupBy(F.window("event_time", "1 day"), F.col("region_id"))
                    .agg(F.sum(F.col("amount")).alias("total_revenue"), F.sum(F.col("quantity")).alias("total_units_sold"),
                        F.approx_count_distinct(F.col("product_id")).alias("uniq_prods_sold"),
                        F.count("*").alias("total_orders"), F.max(F.col("amount")).alias("max_order_val"),
                        F.min(F.col("amount")).alias("min_order_val"))
                    .withColumn("aov", F.round(F.col("total_revenue")/F.col("total_orders"), 2))
                    .withColumn("avg_units_per_prod", F.round(F.col("total_units_sold")/F.col("uniq_prods_sold"), 2))
                    .withColumn("window_start", F.to_date(F.col("window.start")))
                    .withColumn("window_end", F.to_date(F.col("window.end")))
                    .withColumn("week_of_month", F.weekofyear(F.col("window_start"))-
                                            F.weekofyear(
                                                F.date_sub(
                                                    F.col("window_start"),
                                                    F.dayofmonth(F.col("window_start"))+1))+1)
                    .withColumn("is_weekend", F.dayofweek(F.col("window_start")).isin([1,7]))
                    .withColumn("day", F.date_format(F.col("window_start"), "EEEE"))
                    )

    reg_df = spark.read.table("sales_project_streaming.slv.regions")

    joined_df = (daily_df.alias("d").join(reg_df.alias("r"), F.col("d.region_id") == F.col("r.region_id"), "inner")
                .select("d.window_start", "d.window_end", "d.week_of_month", "d.day", "d.is_weekend",
                        F.col("r.region_name").alias("city"), "d.total_revenue", "d.total_units_sold",
                        "d.uniq_prods_sold", "d.total_orders", "d.max_order_val", "d.min_order_val", 
                        "d.avg_units_per_prod")
                )

    query = (joined_df.writeStream.format("delta")
            .option("checkpointLocation", "/Volumes/sales_project_streaming/gld/checkpoints_vol/daily_win_sales_chck/")
            .outputMode("append")
            .trigger(availableNow = True)
            .table("sales_project_streaming.gld.daily_regional_sales_agg"))

    if not query.awaitTermination(200):
        query.stop()

except Exception as e:
    print(f"Error occured while processing stream: {e}")
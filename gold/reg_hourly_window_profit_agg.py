from pyspark.sql import functions as F
import logging

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

try:
    logger.info("Reading Sales Stream......")

    sales_df = (spark.readStream.format("delta")
                .table("sales_project_streaming.slv.sales"))

    logger.info("Reading Expenses Stream......")


    exp_df = (spark.readStream.format("delta")
            .table("sales_project_streaming.slv.expenses"))

    logger.info("Streams Read")

    sales_df = (sales_df.select("region_id", "event_time", "amount", F.lit(0).alias("expense_amount")))

    exp_df = (exp_df.select("region_id", "event_time", F.lit(0).alias("amount"), "expense_amount"))

    combined_df = (sales_df.unionByName(exp_df, allowMissingColumns = True))

    logger.info("Union done......Ready for aggregation. Results loading__________")

    hourly_summ = (combined_df.withWatermark("event_time", "30 minutes")
                .groupBy(F.window("event_time", "1 hour"), F.col("region_id"))
                .agg(F.sum("amount").alias("total_sales"), F.sum("expense_amount").alias("total_expenses"))
                .withColumn("window_start", F.col("window.start"))
                .withColumn("window_end", F.col("window.end"))
                .withColumn("hour", F.hour("window_start"))
                .withColumn("date", F.to_date("window_start"))
                .withColumn("profit", F.col("total_sales") - F.col("total_expenses"))
                .select("window_start", "window_end", "date", "hour", "region_id", "total_sales", "total_expenses",
                        "profit", F.when(F.col("total_sales")!=0, F.round(F.col("profit")/F.col("total_sales"), 2)).otherwise(F.lit(0)).alias("profit_margin"))
                )
    
    logger.info("Aggregation Done.... joining to regions now")

    reg_df = spark.read.table("sales_project_streaming.slv.regions")

    combined_df = (hourly_summ.alias("h").join(reg_df.alias("r"), on = ["region_id"], how = "inner")
                   .select("h.window_start", "h.window_end", "h.date", "h.hour", F.col("r.region_name").alias("city"), 
                           "h.total_sales", "h.total_expenses", "h.profit", "h.profit_margin"))
    

    logger.info("Regions joined, cities fetched.... Writing to sink_________________")

    query = (combined_df.writeStream.format("delta")
            .option("checkpointLocation", "/Volumes/sales_project_streaming/gld/checkpoints_vol/hrly_reg_prof_chck/")
            .outputMode("append")
            .trigger(availableNow = True)
            .table("sales_project_streaming.gld.hourly_regional_profit_agg"))

    if not query.awaitTermination(250):
        query.stop()

except Exception as e:
    logger.error("Error in stream as %s", e, exc_info = True)
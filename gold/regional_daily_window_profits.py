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
    
    logger.info("Finished reading streams")

    sales_df = (sales_df.select("event_time", "region_id", "amount", F.lit(0).alias("expense_amount")))
    exp_df = (exp_df.select("event_time", "region_id", F.lit(0).alias("amount"), "expense_amount"))

    combined_df = sales_df.unionByName(exp_df, allowMissingColumns = True)

    daily_summ = (combined_df.withWatermark("event_time", "1 day")
                  .groupBy(F.window("event_time", "1 day"), F.col("region_id"))
                  .agg(F.sum("amount").alias("total_sales"), F.sum("expense_amount").alias("total_expenses"))
                  .withColumn("profit", F.col("total_sales")-F.col("total_expenses"))
                  .withColumn("window_start", F.col("window.start"))
                  .withColumn("window_end", F.col("window.end"))
                  .withColumn("date", F.to_date("window_start"))
                  .withColumn("profit_margin", F.when(F.col("total_sales")!=0, F.round(F.col("profit")/F.col("total_sales"), 2)).otherwise(F.lit(0)))
                  .select("window_start", "window_end", "date", "region_id", "total_sales", "total_expenses", "profit", "profit_margin")
                  )
    
    logger.info("Aggregation done_________Loading cities")

    reg_df = spark.read.table("sales_project_streaming.slv.regions")

    combined_df = (daily_summ.alias("d").join(reg_df.alias("r"), on = ["region_id"], how = "inner")
                   .select("d.window_start", "d.window_end", "d.date", F.col("r.region_name").alias("city"),
                           "d.total_sales", "d.total_expenses", "d.profit", "d.profit_margin")
                   )
    
    logger.info("Cities loaded_______Writing to Sink. . . . . . . . . ")

    query = (combined_df.writeStream.format("delta")
             .option("checkpointLocation", "/Volumes/sales_project_streaming/gld/checkpoints_vol/daily_reg_prof_win_chck/")
             .outputMode("append")
             .trigger(availableNow = True)
             .table("sales_project_streaming.gld.daily_regional_profit_agg"))
    
    if not query.awaitTermination(250):
        query.stop()
    
    logger.info("Stream Successful...........")
        
except Exception as e:
    logger.error("Error processing stream %s", e, exc_info = True)
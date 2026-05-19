import sys
sys.path.append("/Workspace/Streaming_Kafka_03052026/sales_proj_kafka_DB/transformations")
from time_window_filters import running_current_week_filter
from pyspark.sql import functions as F
import logging

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

weekly_sales_df = (spark.read.format("delta")
                   .table("sales_project_streaming.slv.sales"))

weekly_sales_df = running_current_week_filter(weekly_sales_df)

weekly_exp_df = (spark.read.format("delta")
                 .table("sales_project_streaming.slv.expenses"))

weekly_exp_df = running_current_week_filter(weekly_exp_df)

weekly_exp_df = (weekly_exp_df.groupBy("region_id")
                 .agg(F.sum(F.col("expense_amount")).alias("total_expenses"),
                      F.avg(F.col("expense_amount")).alias("avg_exp_amount"))
                 )

weekly_sales_df = (weekly_sales_df.groupBy("region_id")
                   .agg(F.sum(F.col("amount")).alias("total_revenue"),
                        F.sum(F.col("quantity")).alias("total_units_sold"),
                        F.approx_count_distinct(F.col("product_id")).alias("uniq_prods_sold"),
                        F.count("*").alias("total_sales"),
                        F.max(F.col("amount")).alias("max_order_val"), F.min(F.col("amount")).alias("min_order_val"))
                   )

combined_df = (weekly_sales_df.alias("ws").join(weekly_exp_df.alias("we"), on = ["region_id"], how = "inner")
               .select("ws.region_id", F.col("ws.total_sales").alias("total_sales_orders"), "ws.total_units_sold",
                       "ws.uniq_prods_sold", "ws.max_order_val", "ws.min_order_val", 
                       "we.avg_exp_amount", "ws.total_revenue", "we.total_expenses")
               .withColumn("profit", F.col("total_revenue")-F.col("total_expenses"))
               .withColumn("profit_margin", F.when(F.col("total_revenue") !=0,
                                                    F.round(100.0 * F.col("profit")/F.col("total_revenue"), 2)).otherwise(F.lit(0)))
               )

reg_df = spark.read.table("sales_project_streaming.slv.regions")

combined_df = (combined_df.alias("c").join(F.broadcast(reg_df).alias("r"), on = ["region_id"], how = "inner")
               .select(F.current_date().alias("as_of_date"), F.col("r.region_name").alias("city"), "c.total_sales_orders", "c.total_units_sold", "c.uniq_prods_sold",
                       "c.max_order_val", "c.min_order_val", F.round("c.avg_exp_amount", 2).alias("avg_exp_amount"),
                       "c.total_revenue", "c.total_expenses", "c.profit", "c.profit_margin"))

combined_df.write.format("delta").mode("overwrite").saveAsTable("sales_project_streaming.gld.weekly_running_regional_profit_agg")
from pyspark.sql import functions as F
import sys
sys.path.append("/Workspace/Streaming_Kafka_03052026/sales_proj_kafka_DB/transformations")
from time_window_filters import last_week_filter, add_week_start
from profit_calc import get_profit, profit_margin
import logging

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

try:

    sales_wkly = spark.read.table("sales_project_streaming.slv.sales")
    exp_wkly = spark.read.table("sales_project_streaming.slv.expenses")
    reg_df = spark.read.table("sales_project_streaming.slv.regions")

    logger.info("Created Dataframes")

    sales_wkly = last_week_filter(sales_wkly)
    exp_wkly = last_week_filter(exp_wkly)

    sales_wkly = (sales_wkly.groupBy("region_id")
                .agg(F.sum("amount").alias("total_revenue"), F.count("*").alias("total_sales_orders"),
                    F.sum("quantity").alias("total_units_sold"), F.approx_count_distinct(F.col("product_id")).alias("uniq_prods_sold"),
                    F.max(F.col("amount")).alias("max_order_val"), F.min(F.col("amount")).alias("min_order_val")))

    logger.info("Sales Aggregated")

    exp_wkly = (exp_wkly.groupBy("region_id")
                .agg(F.sum("expense_amount").alias("total_expenses"), F.avg("expense_amount").alias("avg_exp_amount")))

    logger.info("Expenses Aggregated")

    combined_df = (sales_wkly.alias("s").join(exp_wkly.alias("e"), on = ["region_id"], how = "inner")
                .select("s.region_id", "s.total_sales_orders", "s.total_units_sold", "s.uniq_prods_sold",
                        "s.max_order_val", "s.min_order_val", "e.avg_exp_amount", "s.total_revenue", "e.total_expenses"))

    logger.info("Joined Sales & Expenses for each region")

    week_start = spark.sql("select date_trunc('week', current_date() - interval 7 days)").collect()[0][0]

    combined_df = get_profit(combined_df)
    combined_df = profit_margin(combined_df)
    combined_df = combined_df.withColumn("week_start", F.to_date(F.date_trunc("week", F.current_date() - F.expr("INTERVAL 7 DAYS"))))

    joined_df = (combined_df.alias("c").join(reg_df.alias("r"), on =["region_id"], how = "inner")
                .select("c.week_start", F.col("r.region_name").alias("city"), "c.total_sales_orders", "c.total_units_sold", "c.uniq_prods_sold",
                        "c.max_order_val", "c.min_order_val", "c.avg_exp_amount", "c.total_revenue", "c.total_expenses",
                        "c.profit", "c.profit_margin"))

    logger.info("Appending to target table . . . . . . . . . . . . . .")

    joined_df.write.format("delta").mode("overwrite")\
        .option("replaceWhere", f"week_start = '{week_start}'")\
        .saveAsTable("sales_project_streaming.gld.weekly_regional_summary_agg")

    logger.info("Data Successfully appended to Gold table!!")

except Exception as e:
    logger.error("Error occurred as %s", e, exc_info = True)
    raise
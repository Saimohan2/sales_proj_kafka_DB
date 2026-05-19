from pyspark.sql import functions as F
import logging

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

def get_profit(df):

    return df.withColumn("profit", F.col("total_revenue") - F.col("total_expenses"))

def profit_margin(df):

    return df.withColumn("profit_margin", F.when(F.col("total_revenue") !=0,F.round(100.0 * F.col("profit")/F.col("total_revenue"), 2))
                                                .otherwise(F.lit(0)))
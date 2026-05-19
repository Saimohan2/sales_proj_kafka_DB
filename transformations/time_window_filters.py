from pyspark.sql import functions as F

def get_current_week_start():

    return (F.to_date(F.date_trunc("week", F.current_date())))


def add_week_start(df):

    return df.withColumn("week_start", get_current_week_start())

def running_current_week_filter(df):

    return df.filter((F.col("event_date") >= get_current_week_start())
                     & (F.col("event_date") <= F.current_date()))

def last_week_filter(df):

    current_week_start = get_current_week_start()
    last_week_start = F.date_sub(current_week_start, 7)

    return df.filter((F.col("event_date") >= last_week_start) 
                     & (F.col("event_date") < current_week_start))
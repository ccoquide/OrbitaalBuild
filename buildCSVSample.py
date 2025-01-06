import os
import sys
from collections import defaultdict as ddict
import pyspark
from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.functions import from_utc_timestamp, from_unixtime, to_date, sum,count, mean, countDistinct, max,first, stddev
from pyspark import SparkContext
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second, to_utc_timestamp, unix_timestamp, date_format
from pyspark.sql.types import IntegerType,StructField,StructType, LongType, FloatType
from pyspark.sql.functions import col, when, min
from pyspark.sql.functions import collect_list, concat_ws, concat, lit
from pyspark.ml.feature import StringIndexer
from pyspark.sql import Window
from pyspark.sql.functions import row_number    
from pyspark.sql.functions import monotonically_increasing_id, coalesce, date_trunc, regexp_extract
import pandas as pd
import datetime
import pytz
from orbitaal_utils import getTimeDiff

### Init pyspark session
spark = SparkSession \
    .builder \
    .config("spark.driver.memory", "100g") \
    .config("spark.executor.memory", "100g") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.local.dir","/media/ssd4/tmp")\
    .config("spark.driver.maxResultSize","0")\
    .getOrCreate()
spark.sparkContext.uiWebUrl

### Work Path
PATH=sys.argv[1]
date_1={"year":"2009","month":"06","day":"01"}
date_2={"year":"2010","month":"06","day":"01"}
print(f"Build sample for two specific days : {date_1} and {date_2}")

### Writing stream graph samples in csv
pref="orbitaal-stream_graph"
PATH_I=f"{PATH}/STREAM_GRAPH/EDGES"
PATH_O=f"{PATH}/STREAM_GRAPH"
print("Building STREAM GRAPH samples in csv")
net=spark.read.parquet(f'{PATH_I}/orbitaal-stream_graph-date-{date_1["year"]}-*.parquet')\
    .withColumn("year", year(from_unixtime(col("TIMESTAMP"))))\
    .withColumn("month", month(from_unixtime(col("TIMESTAMP"))))\
    .withColumn("day", dayofmonth(from_unixtime(col("TIMESTAMP"))))
net_=net.filter(col("year")==int(date_1["year"])).filter(col("month")==int(date_1["month"])).filter(col("day")==int(date_1["day"])).drop("year","month","day")
net_.coalesce(1).write.mode("overwrite").csv(f'{PATH_O}/{date_1["year"]}_{date_1["month"]}_{date_1["day"]}.csv/', header=True)
net=spark.read.parquet(f'{PATH_I}/orbitaal-stream_graph-date-{date_2["year"]}-*.parquet')\
    .withColumn("year", year(from_unixtime(col("TIMESTAMP"))))\
    .withColumn("month", month(from_unixtime(col("TIMESTAMP"))))\
    .withColumn("day", dayofmonth(from_unixtime(col("TIMESTAMP"))))
net_=net.filter(col("year")==int(date_2["year"])).filter(col("month")==int(date_2["month"])).filter(col("day")==int(date_2["day"])).drop("year","month","day")
net_.coalesce(1).write.mode("overwrite").csv(f'{PATH_O}/{date_2["year"]}_{date_2["month"]}_{date_2["day"]}.csv/', header=True)
for dir in [f'{date_1["year"]}_{date_1["month"]}_{date_1["day"]}.csv',f'{date_2["year"]}_{date_2["month"]}_{date_2["day"]}.csv']:
    PATH_=f'{PATH_O}/{dir}'
    for filename in os.listdir(PATH_):
        if filename.endswith(".csv"):
            os.rename(f'{PATH_}/{filename}',f'{PATH}/{pref}-{dir}')
        else:
            os.remove(f'{PATH_}/{filename}')
    os.rmdir(PATH_)
print("DONE")
### Writing stream graph samples in csv
pref="orbitaal-snapshot"
PATH_I=f"{PATH}/SNAPSHOT/EDGES/day"
PATH_O=f"{PATH}/SNAPSHOT"
print("Building SNAPSHOT samples in csv")
net=spark.read.parquet(f"{PATH_I}/orbitaal-snapshot-date-{date_1['year']}-{date_1['month']}-{date_1['day']}-*.snappy.parquet")
net.coalesce(1).write.mode("overwrite").csv(f'{PATH_O}/{date_1["year"]}_{date_1["month"]}_{date_1["day"]}.csv/', header=True)
net=spark.read.parquet(f"{PATH_I}/orbitaal-snapshot-date-{date_2['year']}-{date_2['month']}-{date_2['day']}-*.snappy.parquet")
net.coalesce(1).write.mode("overwrite").csv(f'{PATH_O}/{date_2["year"]}_{date_2["month"]}_{date_2["day"]}.csv/', header=True)
for dir in [f'{date_1["year"]}_{date_1["month"]}_{date_1["day"]}.csv',f'{date_2["year"]}_{date_2["month"]}_{date_2["day"]}.csv']:
    PATH_=f'{PATH_O}/{dir}'
    for filename in os.listdir(PATH_):
        if filename.endswith(".csv"):
            os.rename(f'{PATH_}/{filename}',f'{PATH}/{pref}-{dir}')
        else:
            os.remove(f'{PATH_}/{filename}')
    os.rmdir(PATH_)
print("DONE")

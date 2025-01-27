from collections import defaultdict as ddict
from pyspark.sql import SparkSession
import sys
import numpy as np
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.functions import countDistinct
from pyspark import SparkContext
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second, to_utc_timestamp, unix_timestamp, date_format
from pyspark.sql.types import StructField, StructType
from pyspark.sql.functions import col, when, sum
from pyspark.sql.functions import collect_list, concat_ws, concat, lit
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import row_number    
from pyspark.sql.functions import monotonically_increasing_id, coalesce, date_trunc, regexp_extract
import datetime
from orbitaal_utils import getTimeDiff, write_SNAPSHOT

### Timezone
timezone="GMT"

### Output file format 
opt="parquet"

### Init pyspark session
spark = SparkSession \
    .builder \
    .config("spark.driver.memory", "100g") \
    .config("spark.executor.memory", "100g") \
    .config("spark.sql.session.timeZone", timezone) \
    .config("spark.local.dir","/media/ssd4/tmp")\
    .config("spark.driver.maxResultSize","0")\
    .getOrCreate()
spark.sparkContext.uiWebUrl
t_0=datetime.datetime.now()

### Work Path
PATH=sys.argv[1]

fname=f"{PATH}/STREAM_GRAPH/EDGES/year*/"

### Reading stream graph as a PySpark DataFrame
net=spark.read.parquet(fname)

#Ordering by TIMESTAMP
net=net.orderBy("TIMESTAMP")

### Writing snapshots
t__0=datetime.datetime.now()
print(f"Writting SNAPSHOTS for years with time zone {timezone}\n")
write_SNAPSHOT(net, PATH, format=opt)
print("SNAPSHOTS wrote in ", getTimeDiff(t__0))
print("Total computing time is ", getTimeDiff(t_0))
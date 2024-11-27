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
from orbitaal_utils import getTimeDiff, write_TEMPORAL
spark = SparkSession \
    .builder \
    .config("spark.driver.memory", "100g") \
    .config("spark.executor.memory", "100g") \
    .config("spark.local.dir","/media/ssd4/tmp")\
    .config("spark.driver.maxResultSize","0")\
    .getOrCreate()
spark.sparkContext.uiWebUrl
t_0=datetime.datetime.now()

### PATH to cleaned data
PATH_I=sys.argv[1]

#Work Path
PATH=sys.argv[2]

### Reading cleaned data as pyspark df
pydf=spark.read.load(PATH_I)

### Building and writing stream graph using new IDs for actors from node table and transactions from cleaned data

write_TEMPORAL(spark, pydf, f"{PATH}/TEMPORAL/network_2009_2021_name/", PATH, "parquet")
print(f"Temporal network computed and written in {getTimeDiff(t_0)}")

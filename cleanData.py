from collections import defaultdict as ddict
from pyspark.sql import SparkSession
import sys
import numpy as np
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.functions import countDistinct
from pyspark import SparkContext
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second, to_utc_timestamp, unix_timestamp, date_format
from pyspark.sql.types import StructField, StructType
from pyspark.sql.functions import col, when
from pyspark.sql.functions import collect_list, concat_ws, concat, lit
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import row_number    
from pyspark.sql.functions import monotonically_increasing_id, coalesce, date_trunc, regexp_extract
import datetime
from orbitaal_utils import CleanData, getFees, getTimeDiff

t_0=datetime.datetime.now()
### Init pyspark session
spark = SparkSession \
    .builder \
    .config("spark.driver.memory", "100g") \
    .config("spark.executor.memory", "100g") \
    .config("spark.local.dir","/media/ssd4/tmp")\
    .config("spark.driver.maxResultSize","0")\
    .getOrCreate()
spark.sparkContext.uiWebUrl

### Path to input data
PATH_I=sys.argv[1]
### Path to output cleaned data
PATH_O=sys.argv[2]

### Reading whole data as pyspark df
selectedColumns=[
    "trID", "nb_inputs", "id_src", "src_cl", "src_identity_radical", "id_dst", "dst_cl", "dst_identity_radical", "value", "valueUSD", "PriceUSD","timestamp", "year", "fee", "bloc", "output_id", "nb_outputs"
    ]
outputs=spark.read.load(PATH_I)\
    .select("trID", "nb_inputs", "id_src", "src_cl", "src_identity_radical", "id_dst", "dst_cl", "dst_identity_radical", "value", "valueUSD", "PriceUSD","timestamp", "year", "fee", "bloc", "output_id", "nb_outputs")

### Handle null src cl, dst cl and duplicated src_best_name and dst_best_name
outputs_=CleanData(outputs)
del(outputs)
print(f" in {getTimeDiff(t_0)}")
t__0=datetime.datetime.now()

### Add rows related to fees transactions and remove transaction associated to value = 0
outputs_=getFees(outputs_)
print(f" in {getTimeDiff(t__0)}")

### Aggregate transactions occuring in the same bloc (the same timestamp)
print("Aggregating all transactions at the finest timestamp resolution (bloc resolution)")
outputs_=outputs_.groupBy("src_cl", "dst_cl", "timestamp", "src_best_name", "dst_best_name", "year")\
    .agg(sum("value").alias("value"), sum("valueUSD").alias("valueUSD"))
    
### Write cleaned data
outputs_.write.save(f"{PATH_O}/Prepared_Data",mode="overwrite")
del(outputs_)

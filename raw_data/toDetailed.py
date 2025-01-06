import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import sort_array, broadcast, collect_list, collect_set,struct, array_sort, explode, posexplode, create_map,from_utc_timestamp, from_unixtime, to_date, sum,count, mean, countDistinct, round
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.types import IntegerType,StructField,StructType, LongType, FloatType, MapType, DateType,StringType,TimestampType,ArrayType
from pyspark.sql.functions import col, when, regexp_replace
import pyspark.sql.functions as f

from pyspark.sql.functions import monotonically_increasing_id, coalesce, date_trunc, regexp_extract
import sys

import os


### Init pyspark session
spark = SparkSession \
    .builder \
    .getOrCreate()
spark.sparkContext.uiWebUrl

### Path to input data
PATH_I=sys.argv[1]
### Path to output cleaned data
PATH_O=sys.argv[2]

def _extract_all_sets(x):
    inputs=json.loads(x.inputs)
    outputs=json.loads(x.outputs)
    ads_ins=[]
    ads_outs=[]

    hash_ins=[]
    values_ins=[]
    ids_ins=[]

    values_outs=[]
    ids_outs=[]

    for i in range(len(inputs)):
        ads1=inputs[i][2]
        if len(ads1)==1:
            ads1=ads1[0]
        else:
            ads1=str(ads1)
        ads_ins.append(ads1)
        hash_ins.append(inputs[i][0])
        ids_ins.append(inputs[i][1])
        values_ins.append(int(inputs[i][3]))
                          
    for i in range(len(outputs)):
        ads1=outputs[i][1]
        if len(ads1)==1:
            ads1=ads1[0]
        else:
            ads1=str(ads1)
        ads_outs.append(ads1)
        ids_outs.append(outputs[i][0])
        values_outs.append(int(outputs[i][2]))
        
    to_return=[(x.hash,x.bloc,ads_ins,hash_ins,values_ins,ids_ins,ads_outs,values_outs,ids_outs,len(inputs),len(outputs),int(x.timestamp),int(x.year),int(x.month),int(x.day))]
    return(to_return)

schema = StructType([
    StructField("hash", StringType(), True),
    StructField("bloc", StringType(), True),
    StructField("ads_ins", ArrayType(StringType()), True),
    StructField("hash_ins", ArrayType(StringType()), True),
    StructField("values_ins", ArrayType(LongType()), True),
    StructField("ids_ins", ArrayType(IntegerType()), True),
    StructField("ads_outs", ArrayType(StringType()), True),
    StructField("values_outs", ArrayType(LongType()), True),
    StructField("ids_outs", ArrayType(LongType()), True),
    StructField("nb_inputs", IntegerType(), True),
    StructField("nb_outputs", IntegerType(), True),
    StructField("timestamp", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
])



original = spark.read.load(PATH_I)

to_write = original.rdd.flatMap(_extract_all_sets).toDF(schema)
#to_write = to_write.withColumn("timestamp",col("timestamp").cast(IntegerType()))#\
                # .withColumn("bloc",col("bloc").cast(IntegerType()))\
                # .withColumn("nb_inputs",col("nb_inputs").cast(IntegerType()))\
                # .withColumn("nb_outputs",col("nb_outputs").cast(IntegerType()))
print(to_write.schema)
to_write.write.save(PATH_O,mode="overwrite")
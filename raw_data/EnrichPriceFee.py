from pyspark.sql import SparkSession

import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType,StructField,StructType, LongType, FloatType, MapType, DateType,StringType,TimestampType,ArrayType

import sys


### Init pyspark session
spark = SparkSession \
    .builder \
    .getOrCreate()
spark.sparkContext.uiWebUrl

### Path to input data
PATH_I=sys.argv[1]
### Path to output data
PATH_O=sys.argv[2]

PATH_PRICE=sys.argv[3]



btc_price = spark.read.csv(PATH_PRICE,header=True)\
    .select("date","PriceUSD","HashRate")\
    .withColumn("PriceUSD",f.col("PriceUSD").cast(FloatType()))\
    .withColumn("HashRate",f.col("HashRate").cast(IntegerType()))
btc_price = btc_price.fillna(0)

before = spark.read.load(PATH_I)

btc_price = btc_price.withColumn("date",f.to_date(f.col("date")))
before= before.withColumn("date",f.to_date(f.from_unixtime("timestamp")))


with_prices = before.join(btc_price,on="date",how="left")
#with_prices = with_prices.withColumn("valueUSD",col("value")*col("PriceUSD")/100000000)


with_prices.write.save(PATH_O,mode="overwrite")
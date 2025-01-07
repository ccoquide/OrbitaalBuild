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
ad2ID=sys.argv[1]
### Path to output data
ad2cl=sys.argv[2]


ADDRESS_BOOK=sys.argv[3]

PATH_DICOad2id2cl2idenity2radical_file=sys.argv[4]


ad2id = spark.read.load(ad2ID).select("ad","id")


ad2cl = spark.read.load(ad2cl)
#convert to long
ad2cl = ad2cl.withColumn("cl",f.col("cl").cast(LongType()))




joined = ad2id.join(ad2cl,on="ad",how="left")
# Step 1: Collect the existing non-null values in 'cl' to avoid duplicates
existing_values = set(joined.select("cl").where(f.col("cl").isNotNull()).distinct().rdd.flatMap(lambda x: x).collect())

# Step 2: Get the maximum existing value or define a starting point
# if existing_values:
#     max_value = max(existing_values)
# else:
#     max_value = 0  # If no existing values, start from 0

# Step 3: Create a column of unique IDs for the null values
df_with_unique_values = joined.withColumn(
    "cl",
    f.when(
        f.col("cl").isNull(),
        -f.monotonically_increasing_id()  # Ensure new IDs are unique
    ).otherwise(f.col("cl"))
)

#df_with_unique_values.write.save(ad2id2cl_file,mode="overwrite")



walletExplorer_hints = spark.read.csv(ADDRESS_BOOK,header=True, sep=",").drop("page")\
    .withColumnRenamed("adresse","ad").withColumnRenamed("service","identity")
print(walletExplorer_hints.show())


#ad2cl=spark.read.load(ad2id2cl_file)
#df_with_unique_values = spark.read.load(ad2id2cl_file)

cl2identity = walletExplorer_hints.join(ad2cl,on="ad",how="left")
#cl2identity = ad2cl.join(walletExplorer_hints,on="ad",how="left")
print(cl2identity[~cl2identity.identity.isNull()].show())
#cl2identity.write.save(ad2cl2identity_known_only,mode="overwrite")

#create_ad2identity(current_dir,walletExplorer_hints,ad2cl,current_dir+"/ad2cl2identity")

#ad2cl=spark.read.load(ad2id2cl_file)
#ad2cl=df_with_unique_values


#cl2identity=spark.read.load(ad2cl2identity_known_only)
cl2identity = cl2identity.groupby("cl").agg(f.concat_ws("_", f.collect_set(f.col("identity"))).alias("identity"))
cl2identity = cl2identity.dropna()
cl2identity = cl2identity.withColumn("identity",f.regexp_replace('identity', 'BTCJam.com-old2_BTCJam.com-old_BTCJam.com', 'BTCJam.com')) #Manual improvement
#cl2identity.write.save(cl2identity_file,mode="overwrite")

#Create cl2identity2identity_radical
#cl2identity = spark.read.load(cl2identity_file)

cl2indentity2radical = cl2identity.withColumn("identity_radical",f.regexp_extract(f.col('identity'), '(.+?)((-(\d+$|(old.*$)|(original.*$)|(cold.*$)|(output.*$)))|$)', 1))
#cl2indentity2radical.write.save(cl2identity2radical_file,mode="overwrite")

#Create ad2cl2identity_all
# After computer clusters

#adID2cl = spark.read.load(df_with_unique_values)
adID2cl=df_with_unique_values
#cl2identity = spark.read.load(cl2identity2radical_file)
cl2identity = cl2indentity2radical
ad2cl2identity = adID2cl.join(cl2identity,on="cl",how="left")
ad2cl2identity.write.save(PATH_DICOad2id2cl2idenity2radical_file,mode="overwrite")

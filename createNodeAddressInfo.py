import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import datetime
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
t_0=datetime.datetime.now()

### PATH to node table
PATH_I_1=sys.argv[1]#directory to node table with old ids
PATH_I_2=sys.argv[2]#directory to node table with new ids
PATH_I_3=sys.argv[3]#directory to address info

###Adding columns with list of public keys related to entities in the node table

#Reading node table with incremental node IDs
table=spark.read.parquet(f"{PATH_I_2}/*.parquet")

#Reading node table with cluster ids (old IDs) used to retrieve list of associated public keys 
table_oldID=spark.read.parquet(f"{PATH_I_1}/*.parquet")\
    .withColumnRenamed("FIRST_TIMESTAMP","FIRST_TIMESTAMP_")\
    .withColumnRenamed("LAST_TIMESTAMP","LAST_TIMESTAMP_")\
    .withColumnRenamed("BALANCE_SATOSHI","BALANCE_SATOSHI_")\
    .drop("NAME")\
    .orderBy("NEW_ID")

#Testing correct matches between two tables and joining both tables
table=table.join(table_oldID,table["ID"]==table_oldID["NEW_ID"],"left").drop("NEW_ID")\
    .withColumn("IS_SAME", (col("FIRST_TIMESTAMP")==col("FIRST_TIMESTAMP_")) & (col("LAST_TIMESTAMP")==col("LAST_TIMESTAMP_")) & (col("BALANCE_SATOSHI")==col("BALANCE_SATOSHI_")))
tst_ok=table[table.IS_SAME==False].count()==0
if not tst_ok:
    print("negative match between two tables from NODE_TABLE/ and TEMPORAL/network_2009_2021_name")
    exit()
print("Positive match")
del(table_oldID)

#Reading addresses information data, adID2cl and ad2id
adID2cl=spark.read.parquet(f"{PATH_I_3}/adID2cl")
ad2id=spark.read.parquet(f"{PATH_I_3}/ad2id")\
    .withColumnRenamed("id","ID_")
print("done")

#joining to get match with cluster id
table=table.join(adID2cl,table.OLD_ID==adID2cl.cl,"left")\
    .withColumn("adID", when(col("OLD_ID")>=0, col("adID")).otherwise(-col("OLD_ID")))
print("done")
add_info=ad2id.join(table,ad2id["ID_"]==table.adID,"left")\
    .select("ID","NAME","ad")\
    .orderBy("ID")
print("done")

#freeing memory
del(adID2cl)
del(ad2id)
del(table)

#Writing address information data
t_1=datetime.datetime.now()
add_info.filter(col("ID").isNotNull())\
    .withColumnRenamed("ad","ADDRESS")\
    .orderBy("ID")
    .coalesce(1).write.mode("overwrite").parquet(f"{PATH_I_2}/add_info/")
del(add_info)

print("Address information added in ",getTimeDiff(t_1))
print("Finish in ", getTimeDiff(t_0))

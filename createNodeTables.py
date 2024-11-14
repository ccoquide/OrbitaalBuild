from collections import defaultdict as ddict
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import datetime
from orbitaal_utils import writeNodeTable, getTimeDiff

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

### Work Path
PATH=sys.argv[1]

### Reading cleaned data as pyspark df
pydf=spark.read.load(f"{PATH}/Prepared_Data")

### Building nodes information table with new IDs for actors (incremental and starting from 0)
writeNodeTable(spark, pydf, PATH)
print(f"Node table computed and written in {getTimeDiff(t_0)}")

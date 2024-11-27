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
import seaborn as sns
import matplotlib.pyplot as plt
import plotly
import plotly.express as px
import datetime
from bitunam_utils import getTimeDiff
#pd.options.plotting.backend = "plotly"
import plotly.io as pio
import pytz

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

### Function definitions
def date_from_path(path):
    """
    Extracts and formats a date from a given file path.

    The function looks for specific date components (year, month, day, hour) in the file path
    and constructs a date string based on the found components. The date components in the path
    should be in the format "component=value", e.g., "year=2023/month=10/day=05/hour=14".

    Args:
        path (str): The file path containing date components.

    Returns:
        str: A formatted date string based on the components found in the path. The format of the
             returned date string will be "%Y", "%Y-%m", "%Y-%m-%d", or "%Y-%m-%d-%H" depending on
             the highest resolution component found in the path.
    """
    reso_=["year", "month", "day","hour"]
    tmp_={}
    tmp_["year"]="%Y"
    tmp_["month"]="%Y-%m"
    tmp_["day"]="%Y-%m-%d"
    tmp_["hour"]="%Y-%m-%d-%H"
    date_=[]
    z="year"
    for r_ in reso_:
        wh=path.find(r_)
        if wh!=-1:
            tmpdate=path.split(r_+"=")[1].split("/")[0]
            if len(tmpdate)==1:
                tmpdate="0"+tmpdate
            date_.append(tmpdate)
            z=r_
        else:
            break
    return "-".join(date_)

def size_h(num, suffix='B'):
    """
    Convert a file size in bytes to a human-readable string with appropriate units.

    Parameters:
    num (float): The file size in bytes.
    suffix (str): The suffix to append to the unit (default is 'B' for bytes).

    Returns:
    str: The human-readable file size string with appropriate units.
    """
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} Yi{suffix}"

### Work Path
PATH=sys.argv[1]
OLDPATH=f"{PATH}/SNAPSHOT/EDGES"
zz=0
zz_=0
DIR=["year","month","day","hour"]
NEWPATH=f"{PATH}/SNAPSHOT_NEW/EDGES"
if not os.path.exists(NEWPATH):
    os.makedirs(NEWPATH)
for r in ["year","month","day","hour"]:
    if not os.path.exists(f"{NEWPATH}/{r}"):
        os.makedirs(f"{NEWPATH}/{r}")
if not os.path.exists(f"{NEWPATH}/ALL"):
    os.makedirs(f"{NEWPATH}/ALL")
for dirpath,dirnames,filenames in os.walk(OLDPATH):
    if dirpath!=OLDPATH:
        for f in filenames:
            fp=os.path.join(dirpath,f)
            if not os.path.islink(fp):
                if fp.endswith(".parquet"):
                    ext=date_from_path(fp)
                    old=fp
                    new=[f'{NEWPATH}/{DIR[len(ext.split("-"))-1]}/orbitaal-snapshot-date-{ext}-file-id-{zz+1}.snappy.parquet', f'{NEWPATH}/ALL/orbitaal-snapshot-part-{zz_+1}.snappy.parquet'][int("ALL" in dirpath)]
                    os.rename(old,new)
                    zz+=1
df=spark.read.parquet("/media/ssd4/celestin/SNAPSHOT_NEW/EDGES/year/*.parquet")
df=df.groupBy("SRC_ID","DST_ID").agg(sum("VALUE_SATOSHI").alias("VALUE_SATOSHI"),sum("VALUE_USD").alias("VALUE_USD"))
df.coalesce(1).write.mode("overwrite").parquet("/media/ssd4/celestin/SNAPSHOT_NEW/EDGES/ALL")
for dirpath, dirnames,filenames in os.walk(f'{NEWPATH}/ALL'):
    for f in filenames:
        if f.endswith(".parquet"):
            os.rename(os.path.join(dirpath,f),os.path.join(f'{NEWPATH}/ALL',"orbitaal-snapshot-all.snappy.parquet"))
        else:
            os.remove(os.path.join(dirpath,f))#Remove non-necessary spark related files
print("Files renamed and moved within", getTimeDiff(t_0))
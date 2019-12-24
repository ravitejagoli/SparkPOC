from operator import *
from pyspark import *
from pyspark.sql import *
from py4j.protocol import Py4JJavaError



#Setting Spark cluster
#sc = SparkContext('yarn-client')
#spark = HiveContext(sc)

spark = SparkSession.builder\
        .appName("ADFSpark")\
		.enableHiveSupport()\
		.config("hive.exec.dynamic.partition", "true")\
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
	.config("spark.sql.parquet.writeLegacyFormat", "true")\
        .getOrCreate()

from pyspark.sql.functions import *

spark.sql("Select 'Hi'")
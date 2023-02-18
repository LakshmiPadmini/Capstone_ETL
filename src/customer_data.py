#import findspark
#findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField,StringType,IntegerType,StructType,TimestampType)
from pyspark.sql.functions import *
import pandas as pd

spark = SparkSession.builder.appName('Custmer data').getOrCreate()
df = spark.read.json("../../dataset/creditcard_dataset/cdw_sapp_custmer.json")  

df.show()
df.printSchema()
df.columns
df.describe().show()
df = df.withColumn("SSN",col("SSN").cast(IntegerType()))\
       .withColumn("CUST_ZIP",col("CUST_ZIP").cast(IntegerType()))\
       .withColumn("LAST_UPDATED",col("LAST_UPDATED").cast(TimestampType()))\
       .withColumn("CUST_PHONE",col("CUST_PHONE").cast(StringType())) 
df = df.withColumn("FIRST_NAME", initcap(col('FIRST_NAME'))).withColumn("LAST_NAME",initcap(col('LAST_NAME')))
df = df.withColumn('MIDDLE_NAME',lower(col('MIDDLE_NAME')))
df = df.withColumn("FULL_STREET_ADDRESS", concat_ws(",",col('APT_NO'),col('STREET_NAME'))).drop("APT_NO").drop("STREET_NAME")
df = df.withColumn("CUST_PHONE", regexp_replace(df.CUST_PHONE, "(\d{3})(\d{3})(\d{1})", "($1) $2-$3"))
df.printSchema()
df.collect()





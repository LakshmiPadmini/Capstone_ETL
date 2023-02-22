#import findspark
#findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField,StringType,IntegerType,StructType,TimestampType)
from pyspark.sql.functions import *
import pandas as pd

spark = SparkSession.builder.appName('Custmer data').getOrCreate()
df_customer = spark.read.json("../../dataset/creditcard_dataset/cdw_sapp_custmer.json")

df_customer.show()
df_customer.printSchema()
df_customer.columns
df_customer.describe().show()
df_customer = df_customer.withColumn("SSN",col("SSN").cast(IntegerType()))\
       .withColumn("CUST_ZIP",col("CUST_ZIP").cast(IntegerType()))\
       .withColumn("LAST_UPDATED",col("LAST_UPDATED").cast(TimestampType()))\
       .withColumn("CUST_PHONE",col("CUST_PHONE").cast(StringType())) 
df_customer = df_customer.withColumn("FIRST_NAME", initcap(col('FIRST_NAME'))).withColumn("LAST_NAME",initcap(col('LAST_NAME')))
df_customer = df_customer.withColumn('MIDDLE_NAME',lower(col('MIDDLE_NAME')))
df_customer = df_customer.withColumn("FULL_STREET_ADDRESS", concat_ws(",",col('APT_NO'),col('STREET_NAME'))).drop("APT_NO").drop("STREET_NAME")
df_customer = df_customer.withColumn("CUST_PHONE", regexp_replace(df_customer.CUST_PHONE, "(\d{3})(\d{3})(\d{1})", "($1) $2-$3"))
df_customer.printSchema()
df_customer.show()
df_customer.write.format("jdbc").mode("append")\
        .option("url","jdbc:mysql://localhost:3306/creditcard_capstone")\
        .option("dbtable","creditcard_capstone.CDW_SAPP_CUSTOMER")\
        .option("user","root")\
        .option("password","password")\
        .save()





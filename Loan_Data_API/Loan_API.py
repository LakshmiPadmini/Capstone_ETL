import findspark
findspark.init()
import requests
import json
from pprint import pp
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
spark = SparkSession.builder.master("local[1]").appName('ApiData.com').getOrCreate()
baseurl = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
def main_request(baseurl):
    r = requests.get(baseurl)
    print(r.status_code)
    data = r.json()
    return data


data = main_request(baseurl)
pp(data)
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF()
df.show()
df.write.format("jdbc").mode("overwrite")\
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone")\
        .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application")\
        .option("user", "root")\
        .option("password", "password")\
        .save()
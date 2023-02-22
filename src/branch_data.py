import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, TimestampType)
from pyspark.sql.functions import *

class BranchData:
    def __init__(self, path=None):
        self.path = path
        self.df = None

    def load_data(self):
        df = self.extract_data()
        df = self.transform_branch_data(df)
        self.df = df
        # Load data into


        return df
    def extract_data(self):
        spark = SparkSession.builder.appName('SparkByBranch').getOrCreate()
        return spark.read.json(self.path)

    def transform_branch_data(self, df):
        df = df.withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType()))\
               .withColumn("BRANCH_ZIP", col("BRANCH_ZIP").cast(IntegerType()))\
               .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))
        df = df.withColumn("BRANCH_PHONE", regexp_replace(df.BRANCH_PHONE, "(\d{3})(\d{3})(\d{4})", "($1) $2-$3"))
        df = df.na.fill(00000, subset=["BRANCH_ZIP"])
        return df

    def get_data(self):
        return self.df

    def load_database(self,df):
        self.df.write.format("jdbc").mode("append") \
            .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
            .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
            .option("user", "root") \
            .option("password", "password") \
            .save()

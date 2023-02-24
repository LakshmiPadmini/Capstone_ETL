import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, TimestampType)
from pyspark.sql.functions import *


class BranchData:
    def __init__(e):
        pass

    def load_data(self, path=None,write_to_db=False, mode="append"):
        spark = SparkSession.builder.appName('SparkByBranch').getOrCreate()
        df = spark.read.json(path)
        df = self.transform_branch_data(df)
        if write_to_db:
            self.write_to_database(df, mode=mode)
        return df

    def transform_branch_data(self, df):
        df = df.withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType())) \
            .withColumn("BRANCH_ZIP", col("BRANCH_ZIP").cast(IntegerType())) \
            .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))
        df = df.withColumn("BRANCH_PHONE", regexp_replace(df.BRANCH_PHONE, "(\d{3})(\d{3})(\d{4})", "($1) $2-$3"))
        df = df.na.fill(00000, subset=["BRANCH_ZIP"])
        return df

    def write_to_database(self, df, mode="append"):
        df.write.format("jdbc").mode(mode) \
            .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
            .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
            .option("user", "root") \
            .option("password", "password") \
            .save()

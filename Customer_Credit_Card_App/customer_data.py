from pyspark.sql import SparkSession
from pyspark.sql.types import (StringType, IntegerType, TimestampType)
from pyspark.sql.functions import *
import secret


class CustomerData:
    def __init__(self):
        pass

    # Extract,Transform,Load OF Customer Json Format Data Into MySql Database CreditCard_Capstone.
    def load_data(self, path, write_to_db=False, mode="append"):
        spark = SparkSession.builder.appName('Customer data').getOrCreate()
        df_customer = spark.read.json(path)
        df_customer = self.transform_data(df_customer)
        if write_to_db:
            self.write_to_database(df_customer, mode=mode)
        return df_customer

    def transform_data(self, df_customer):
        df_customer = df_customer.withColumn("SSN", col("SSN").cast(IntegerType())) \
            .withColumn("CUST_ZIP", col("CUST_ZIP").cast(IntegerType())) \
            .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType())) \
            .withColumn("CUST_PHONE", col("CUST_PHONE").cast(StringType()))
        df_customer = df_customer.withColumn("FIRST_NAME", initcap(col('FIRST_NAME'))).withColumn("LAST_NAME", initcap(
            col('LAST_NAME')))
        df_customer = df_customer.withColumn('MIDDLE_NAME', lower(col('MIDDLE_NAME')))
        df_customer = df_customer.withColumn("FULL_STREET_ADDRESS",
                                             concat_ws(",", col('APT_NO'), col('STREET_NAME'))).drop("APT_NO").drop(
            "STREET_NAME")
        df_customer = df_customer.withColumn("CUST_PHONE",
                                             regexp_replace(df_customer.CUST_PHONE, "(\d{3})(\d{3})(\d{1})",
                                                            "($1) $2-$3"))
        return df_customer

    def write_to_database(self, df_customer, mode="append"):
        df_customer.write.format("jdbc").mode(mode) \
            .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
            .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER") \
            .option("user", secret.user) \
            .option("password", secret.password) \
            .save()

import findspark

from utils import secret

findspark.init()
from pyspark.sql import SparkSession
import pandas as pd


class CreditData:
    def __init__(self):
        pass
    # Extract,Transform,Load OF Credit_Card Json Format Data Into MySql Database CreditCard_Capstone.
    def load_data(self, path, write_to_db=False, mode="append"):
        df = pd.read_json(path, lines=True)
        df = self.transform_credit_data(df)
        if write_to_db:
            self.write_to_database(df, mode=mode)
        return df

    def transform_credit_data(self, df):
        df = df.rename(columns={"CREDIT_CARD_NO": "CUST_CC_NO"})
        df["TRANSACTION_TYPE"] = df["TRANSACTION_TYPE"].astype(str)
        df["TRANSACTION_VALUE"] = df["TRANSACTION_VALUE"].astype(float)
        df["CUST_CC_NO"] = df["CUST_CC_NO"].astype(str)
        df['date'] = pd.to_datetime(df[['YEAR', 'MONTH', 'DAY']], format='%d-%m-%Y')
        df['TIMEID'] = df['date'].dt.strftime('%Y%m%d')
        df = df.drop(['DAY', "MONTH", "YEAR"], axis=1)
        df = df.drop("date", axis=1)
        spark = SparkSession.builder.appName('SparkByCredit').getOrCreate()
        spark_df = spark.createDataFrame(df)
        return spark_df

    def write_to_database(self, df, mode="append"):
        df.write.format("jdbc").mode(mode) \
            .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
            .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
            .option("user", secret.user) \
            .option("password", secret.password) \
            .save()


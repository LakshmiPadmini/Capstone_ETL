import findspark
findspark.init()

import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import pandas.io.sql as psql
import seaborn as sns
import numpy as np
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName('ApiData.com').getOrCreate()
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='password',
    database='creditcard_capstone'
)

cursor = connection.cursor()
sql = "SELECT * FROM CDW_SAPP_loan_application"
cursor.execute(sql)
result_set = cursor.fetchall()
data_frame = psql.read_sql(sql, con=connection)
pd.set_option("display.max_columns", None)
#print(data_frame.head())


cursor.close()
connection.close()
spark_df = spark.createDataFrame(data_frame)

###Find and plot the percentage of applications approved for self-employed applicants.
data_api = data_frame.groupby(['Self_Employed', 'Application_Status'])['Application_ID'].count()
data_api = data_api.reset_index()
total_applications = len(data_frame)
data_api = data_api[(data_api['Application_Status'] == 'Y') & (data_api['Self_Employed'] == 'Yes')]
print(data_api)
data_api['Count_Percent'] = data_api['Application_ID'] / total_applications * 100
print("Percentage of applications approved for self-employed applicants: ", data_api['Count_Percent'])
#data_api
sns.set(rc={"figure.figsize": (4, 6)})
sns.set_theme(style="whitegrid", palette="hls")
sns.barplot(x='Application_Status',
            y='Count_Percent',
            hue='Self_Employed',
            data=data_api).set(title="Percentage of applications approved for self-employed applicants.")

# Show the plot
plt.show()
###Find the percentage of rejection for married male applicants.
df_maarried_male = data_frame.groupby(["Married","Application_Status","Gender"])["Application_ID"].count()
df_maarried_male = df_maarried_male.reset_index()
print(df_maarried_male)
total_applications = len(data_frame)
df_maarried_male = df_maarried_male[(df_maarried_male['Married'] == 'Yes') & (df_maarried_male['Application_Status'] == 'N' ) & (df_maarried_male['Gender'] == 'Male')]
df_maarried_male['Count_Percent'] = df_maarried_male['Application_ID']/total_applications * 100
#df_maarried_male
sns.set(rc={"figure.figsize":(4,6)})
sns.set_theme(style="whitegrid", palette="hls")
sns.barplot(x ='Married',
            y = 'Count_Percent',
            hue='Gender',
            data = df_maarried_male).set(title ="Percentage of rejection for married male applicants." )



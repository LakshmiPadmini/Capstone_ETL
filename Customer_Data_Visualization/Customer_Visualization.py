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
import datetime as dt
spark = SparkSession.builder.master("local[1]").appName('Credit_card.com').getOrCreate()
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='password',
    database='creditcard_capstone'
)
cursor = connection.cursor()
sql = "SELECT * FROM cdw_sapp_credit_card"
cursor.execute(sql)
result_set = cursor.fetchall()
data_frame = psql.read_sql(sql, con=connection)
pd.set_option("display.max_columns", None)
#print(data_frame.head())
#data_frame
cursor.close()
connection.close()
spark_df = spark.createDataFrame(data_frame)
###Find and plot which transaction type has a high rate of transactions.

spark_df = spark_df.select('TRANSACTION_TYPE')
pandas_df = spark_df.toPandas()
plt.rcParams["figure.figsize"] = [8, 4]
plt.rcParams["figure.autolayout"] = True
plt.plot(pandas_df['TRANSACTION_TYPE'].value_counts(), ls='-.', c='r', marker='o')
plt.title('High Rate Transaction Type')
plt.ylabel('Number of Transactions')
plt.xlabel('Transaction Type')
plt.text(6, 6850, ' Bills occurs most often', fontsize = 10,
      color ='black', ha ='right', va ='top',
         alpha = 1.0)
plt.show()
###plot the top three months with the largest transaction data.
spark_df = spark_df.select('TIMEID', 'TRANSACTION_VALUE')
panda_df = spark_df.toPandas()
panda_df["month"] = pd.to_datetime(panda_df["TIMEID"],format='%Y%m%d').dt.month
print(panda_df['month'])
spark_df = spark_df.select('TIMEID', 'TRANSACTION_VALUE')
panda_df = spark_df.toPandas()
panda_df["month"] = pd.to_datetime(panda_df["TIMEID"],format='%Y%m%d').dt.month
print(panda_df['month'])

df_months = panda_df.groupby('month')['TRANSACTION_VALUE'].sum().reset_index()
print(df_months)
df_months = df_months.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
#df_months = df_months.sort_values(by=['month'])
df_months = df_months[:3]
df_months = df_months.sort_values(by=['month'],ascending=True)
print(df_months)
plt.rcParams["figure.figsize"] = [8, 5]
plt.rcParams["figure.autolayout"] = True
x = ['May', 'Oct', 'Dec']
default_x_ticks = range(len(x))
plt.plot(default_x_ticks, df_months['TRANSACTION_VALUE'], ls='-', c='g', lw='3', marker='o')
plt.title('Top Three Months With Highest Number Of Transactions')
plt.xticks(default_x_ticks, x)
plt.ylabel('Transaction Amount')
plt.xlabel('Months')
plt.show()
###plot which branch processed the highest total dollar value of healthcare transactions.
spark_df = spark_df.select('BRANCH_CODE', 'TRANSACTION_VALUE').filter(spark_df.TRANSACTION_TYPE == 'Healthcare')
panda_df = spark_df.toPandas()
df = panda_df.groupby('BRANCH_CODE')['TRANSACTION_VALUE'].sum().reset_index()
df = df.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
df = df[:5]
df_top5 = df.sort_values(by=['BRANCH_CODE'], ascending=True)
print(df)
top_five = df_top5['TRANSACTION_VALUE']
colors = ['grey' if (s < max(top_five)) else 'red' for s in top_five]

fig, ax = plt.subplots(figsize=(6,5))
sns.set_style('white')
ax=sns.barplot(x='BRANCH_CODE', y='TRANSACTION_VALUE',
               data=df_top5, palette=colors)
plt.title('Branch which has Highest Number Of Healthcare Transactions', fontsize=12)
plt.xlabel('Branch Code')
plt.xticks(fontsize=16)
plt.ylabel('Transaction Amount', fontsize=12)
plt.yticks(fontsize=15)
ax.text(x=0.9, y=0.9, s='Branch 25 with highest total healthcare transaction',
        color='red', size=10, weight='bold')
sns.despine(bottom=True)
ax.grid(False)
ax.tick_params(bottom=False, left=True)
plt.show()
###Customer_plots
sql = "SELECT * FROM cdw_sapp_customer"
cursor.execute(sql)
result_set = cursor.fetchall()
data_frame = psql.read_sql(sql, con=connection)
pd.set_option("display.max_columns", None)
#print(data_frame.head())
###Find and plot which state has a high number of customers.
spark_df= spark_df.select("CUST_STATE")
pandas_df = spark_df.toPandas()
pandas_df = pandas_df['CUST_STATE'].value_counts()
ax=pandas_df.plot(kind="barh",rot=0, figsize=(10,7),width=0.7,
               color='pink')
ax.grid(b = True, color = 'grey',linestyle = '-.',linewidth=0.5,alpha=0.9)
ax.invert_yaxis()
for i in ax.patches:
    plt.text(i.get_width()+0.2, i.get_y()+0.5,
             str(round((i.get_width()), 2)),
             fontsize = 10, fontweight = 'bold',
             color = 'grey')
plt.text(0.9,0.15,'NEWYORK STATE WITH HIGHEST NUMBER OF CUSTOMERS', fontsize=10, color='black',
         ha='left', va='center', alpha=0.9)
###Find and plot the sum of all transactions for the top 10 customers,
##and which customer has the highest transaction amount. hint(use CUST_SSN).

sql = "SELECT c.FIRST_NAME,c.LAST_NAME,c.MIDDLE_NAME,c.SSN,c.CUST_EMAIL,cc.CUST_SSN,cc.TRANSACTION_VALUE FROM cdw_sapp_customer c  JOIN cdw_sapp_credit_card cc ON c.SSN=cc.CUST_SSN "
cursor.execute(sql)
result_set = cursor.fetchall()
data_frame = psql.read_sql(sql, con=connection)
pd.set_option("display.max_columns", None)
#print(data_frame.head())
spark_df= spark_df.select('SSN', 'FIRST_NAME', 'LAST_NAME', 'TRANSACTION_VALUE')
pandas_df = spark_df.toPandas()
#print(pandas_df)
pandas_df = pandas_df.groupby('FIRST_NAME')['TRANSACTION_VALUE'].sum().reset_index()
print(pandas_df)
pandas_df = pandas_df.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
print(pandas_df)

pandas_df = pandas_df[:10]
spark_df= spark_df.select('SSN', 'FIRST_NAME', 'LAST_NAME', 'TRANSACTION_VALUE')
pandas_df = spark_df.toPandas()
pandas_df = pandas_df.groupby('FIRST_NAME')['TRANSACTION_VALUE'].sum().reset_index()
pandas_df = pandas_df.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
pandas_df = pandas_df[:10]
plt.rcParams["figure.figsize"] = [20,10]
plt.rcParams["figure.autolayout"] = True
plt.plot(pandas_df['FIRST_NAME'], pandas_df['TRANSACTION_VALUE'], ls=':', c='red', marker='o')
plt.xticks(pandas_df['FIRST_NAME'])
plt.title('Customer With Highest Transaction Amount.')
plt.text(0.9, 15000, 'Alexis with $15134.35 Transaction Amount', fontsize=15,color='green', ha='left', va='bottom',
        alpha=1.0)
plt.ylabel('Transaction Amount')
plt.xlabel('Customer Names')

plt.show()
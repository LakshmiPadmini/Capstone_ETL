import findspark

findspark.init()

import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import pandas.io.sql as psql
import seaborn as sns
import pyinputplus as pyip
from pyspark.sql import SparkSession
import warnings

warnings.filterwarnings('ignore')
spark = SparkSession.builder.master("local[1]").appName('Credit_card.com').getOrCreate()
pd.set_option("display.max_columns", None)

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='password',
    database='creditcard_capstone'
)


def get_credit_card_df():
    sql = "SELECT * FROM cdw_sapp_credit_card"
    return psql.read_sql(sql, con=connection)

def get_customer_df():
    sql = "SELECT * FROM cdw_sapp_customer"
    return psql.read_sql(sql, con=connection)
def get_highest_transaction():
    sql = "SELECT c.FIRST_NAME,c.LAST_NAME,c.MIDDLE_NAME,c.SSN,c.CUST_EMAIL,cc.CUST_SSN,cc.TRANSACTION_VALUE" \
          " FROM cdw_sapp_customer c " \
          "JOIN cdw_sapp_credit_card cc ON c.SSN=cc.CUST_SSN "
    return psql.read_sql(sql, con=connection)


def plot_highest_transactions(df):
    spark_df = spark.createDataFrame(df)
    spark_df = spark_df.select('TIMEID', 'TRANSACTION_VALUE')
    panda_df = spark_df.toPandas()
    panda_df["month"] = pd.to_datetime(panda_df["TIMEID"], format='%Y%m%d').dt.month
    print(panda_df['month'])

    df_months = panda_df.groupby('month')['TRANSACTION_VALUE'].sum().reset_index()
    print(df_months)
    df_months = df_months.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
    # df_months = df_months.sort_values(by=['month'])
    df_months = df_months[:3]
    df_months = df_months.sort_values(by=['month'], ascending=True)
    print(df_months)
    plt.rcParams["figure.figsize"] = [8, 5]
    plt.rcParams["figure.autolayout"] = True
    x = ['May', 'Oct', 'Dec']
    # y = [196568.87,201086.67,196488.59,194203.25,201310.26,195468.74,201199.35,196453.41,196069.44,202583.89,200549.36,201251.08]
    default_x_ticks = range(len(x))
    plt.plot(default_x_ticks, df_months['TRANSACTION_VALUE'], ls='-', c='g', lw='3', marker='o')
    plt.title('Top Three Months With Highest Number Of Transactions')
    plt.xticks(default_x_ticks, x)
    plt.ylabel('Transaction Amount')
    plt.xlabel('Months')
    plt.show()

def plot_highest_healthcare_transactions(df):
    spark_df = spark.createDataFrame(df)
    spark_df = spark_df.select('BRANCH_CODE', 'TRANSACTION_VALUE').filter(spark_df.TRANSACTION_TYPE == 'Healthcare')
    panda_df = spark_df.toPandas()
    df = panda_df.groupby('BRANCH_CODE')['TRANSACTION_VALUE'].sum().reset_index()
    df = df.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
    df = df[:5]
    df_top5 = df.sort_values(by=['BRANCH_CODE'], ascending=True)
    print(df)

    top_five = df_top5['TRANSACTION_VALUE']
    colors = ['grey' if (s < max(top_five)) else 'red' for s in top_five]

    fig, ax = plt.subplots(figsize=(6, 5))
    sns.set_style('white')
    ax = sns.barplot(x='BRANCH_CODE', y='TRANSACTION_VALUE',
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

def plot_highest_number_customer(df):
    spark_df = spark.createDataFrame(df)
    spark_df = spark_df.select("CUST_STATE")
    pandas_df = spark_df.toPandas()
    pandas_df = pandas_df['CUST_STATE'].value_counts()
    ax = pandas_df.plot(kind="barh", rot=0, figsize=(10, 7), width=0.7,
                        color='pink')
    ax.grid(b=True, color='grey', linestyle='-.', linewidth=0.5, alpha=0.9)
    ax.invert_yaxis()
    for i in ax.patches:
        plt.text(i.get_width() + 0.2, i.get_y() + 0.5,
                 str(round((i.get_width()), 2)),
                 fontsize=10, fontweight='bold',
                 color='grey')
    plt.text(0.9, 0.15, 'NEWYORK STATE WITH HIGHEST NUMBER OF CUSTOMERS', fontsize=10, color='black', ha='left',
             va='center', alpha=0.7)

    plt.show()
def plot_highest_transaction_amount(df):
    spark_df = spark.createDataFrame(df)
    spark_df = spark_df.select('SSN', 'FIRST_NAME', 'LAST_NAME', 'TRANSACTION_VALUE')
    pandas_df = spark_df.toPandas()
    # print(pandas_df)
    pandas_df = pandas_df.groupby('FIRST_NAME')['TRANSACTION_VALUE'].sum().reset_index()
    #print(pandas_df)
    pandas_df = pandas_df.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
    #print(pandas_df)
    pandas_df = pandas_df[:10]
    plt.rcParams["figure.figsize"] = [10, 10]
    plt.rcParams["figure.autolayout"] = True
    plt.plot(pandas_df['FIRST_NAME'], pandas_df['TRANSACTION_VALUE'], ls='-', c='red', marker='o')
    plt.xticks(pandas_df['FIRST_NAME'])
    plt.title('Customer With Highest Transaction Amount.')
    plt.text(0.9, 15000, 'Alexis with $15134.35 Transaction Amount', fontsize=15,
             color='green', ha='left', va='bottom',
             alpha=1.0)
    plt.ylabel('Transaction Amount')
    plt.xlabel('Customer Names')

    plt.show()
def plot_highest_transaction_type(df):
    spark_df = spark.createDataFrame(df)
    spark_df = spark_df.select('TRANSACTION_TYPE')
    pandas_df = spark_df.toPandas()
    plt.rcParams["figure.figsize"] = [8, 4]
    plt.rcParams["figure.autolayout"] = True
    plt.plot(pandas_df['TRANSACTION_TYPE'].value_counts(), ls='-.', c='r', marker='o')
    plt.title('High Rate Transaction Type')
    plt.ylabel('Number of Transactions')
    plt.xlabel('Transaction Type')
    plt.text(6, 6850, ' Bills occurs most often', fontsize=10,
             color='black', ha='right', va='top',
             alpha=1.0)
    plt.show()

def main():
    credit_card_df = get_credit_card_df()
    customer_df = get_customer_df()
    high_transaction = get_highest_transaction()
    while True:
        print("#" * 50)
        print("Customer Data Visualization Menu")
        print("#" * 50)
        print("1. Credit Card Highest Number of TransactionPlot ")
        print("2. Highest Number of Healthcare transaction by Branch")
        print("3. Highest Number of Customers in a State")
        print("4. Transaction Type with high rate of transaction")
        print("5. Customers With Highest Transaction Amount")
        print("6. Quit")
        choice = pyip.inputInt("Enter a Menu Choice: ", min=1, max=6)
        if choice == 1:
            plot_highest_transactions(credit_card_df)
        elif choice == 2:
            plot_highest_healthcare_transactions(credit_card_df)
        elif choice == 3:
            plot_highest_number_customer(customer_df)
        elif choice == 4:
            plot_highest_transaction_type(credit_card_df)
        elif choice == 5:
            plot_highest_transaction_amount(high_transaction)

        else:
            break

if __name__ == "__main__":
    main()

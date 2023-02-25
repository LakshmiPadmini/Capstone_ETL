import findspark


findspark.init()

import pymysql
import pandas.io.sql as psql
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, month, year, dayofmonth
import regex as re
import pyinputplus as pyip
import warnings

warnings.filterwarnings('ignore')

pd.set_option("display.max_columns", None)
spark = SparkSession.builder.master("local[1]").appName('Credit_card.com').getOrCreate()

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='password',
    database='creditcard_capstone'
)
cursor = connection.cursor()


def transaction_menu_1_1(zipcode=21042, mm=11, yy=2018):
    sql = "SELECT c.CUST_ZIP,c.SSN,cc.CUST_SSN,cc.TRANSACTION_VALUE,cc.TRANSACTION_TYPE,cc.TIMEID FROM " \
          "cdw_sapp_customer c  JOIN cdw_sapp_credit_card cc ON c.SSN=cc.CUST_SSN"
    cursor.execute(sql)
    # result_set = cursor.fetchall()
    data_frame = psql.read_sql(sql, con=connection)
    pd.set_option("display.max_columns", None)

    data_frame["TIMEID"] = pd.to_datetime(data_frame["TIMEID"], format='%Y%m%d')
    customer_transaction = data_frame[(data_frame.CUST_ZIP == zipcode) &
                                      (data_frame.TIMEID.dt.month == mm) &
                                      (data_frame.TIMEID.dt.year == yy)]
    daily_values = customer_transaction.groupby(customer_transaction.TIMEID.dt.day)["TRANSACTION_VALUE"].sum()
    result = pd.DataFrame({"Day": daily_values.index, "Transaction Value": daily_values.values})
    result = result.sort_values("Day", ascending=False)

    print(result)


def transaction_mod2(transaction_type='Bills'):
    sql = "SELECT c.CUST_ZIP,c.SSN,cc.CUST_SSN,cc.TRANSACTION_VALUE,cc.TRANSACTION_TYPE,cc.TIMEID \
           FROM cdw_sapp_customer c  JOIN cdw_sapp_credit_card cc ON c.SSN=cc.CUST_SSN "
    cursor.execute(sql)
    data_frame = psql.read_sql(sql, con=connection)

    transaction_type = data_frame[(data_frame['TRANSACTION_TYPE'].str.lower() == transaction_type.lower()) & (data_frame['TRANSACTION_VALUE'])]
    transaction_df = transaction_type.groupby('TRANSACTION_TYPE').agg(count=('TRANSACTION_TYPE', 'count'),
                                                                      value=('TRANSACTION_VALUE', 'sum'))
    transaction_df['value'] = round(transaction_df['value'], 2)
    print(transaction_df)


# transaction_mod2()
def transaction_mod3(branch_state = 'PA'):
    sql = "SELECT bc.BRANCH_CODE,bc.BRANCH_STATE,cc.TRANSACTION_VALUE,cc.TRANSACTION_TYPE,cc.BRANCH_CODE \
               FROM cdw_sapp_branch bc JOIN cdw_sapp_credit_card cc ON bc.BRANCH_CODE=cc.BRANCH_CODE"
    data_frame = psql.read_sql(sql, con=connection)
    branch_transaction = data_frame[(data_frame['BRANCH_STATE'] == branch_state) & (data_frame['TRANSACTION_VALUE'])]
    trans_by_branch_state = branch_transaction.groupby(['BRANCH_STATE']).agg(
        total_transactions=('TRANSACTION_VALUE', 'sum'))
    print(trans_by_branch_state)


def customer_mod1(ssn=None):
    sql = "SELECT * FROM cdw_sapp_customer "
    data_frame = psql.read_sql(sql, con=connection)
    selected_cols = ['SSN', 'FIRST_NAME', 'LAST_NAME', 'MIDDLE_NAME', 'CUST_COUNTRY', 'CUST_CITY', 'CUST_EMAIL',
                     'CUST_PHONE', 'FULL_STREET_ADDRESS', 'CUST_STATE', 'CUST_ZIP', 'CREDIT_CARD_NO']
    result_df = data_frame.loc[data_frame['SSN'] == ssn, selected_cols]
    print(result_df)


def customer_mod2(ccn=None):
    sql = "SELECT * FROM cdw_sapp_customer "
    data_frame = psql.read_sql(sql, con=connection)
    pd.set_option("display.max_columns", None)
    spark_df = spark.createDataFrame(data_frame)
    # todo get user input
    print('Module Two:\n\tModify existing account details of a customer by entering customers credit card number\n')
    cust_fields = spark_df.select('SSN', 'FIRST_NAME', 'LAST_NAME', 'MIDDLE_NAME', 'CUST_COUNTRY', 'CUST_CITY',
                                  'CUST_EMAIL', 'CUST_PHONE', 'FULL_STREET_ADDRESS', 'CUST_STATE', 'CUST_ZIP',
                                  'CREDIT_CARD_NO').filter(spark_df.CREDIT_CARD_NO == ccn)
    cust_fields.distinct().show()
    # todo ask user what to modify
    column_name = pyip.inputStr('Enter the name of the column: ')
    old_value = pyip.inputStr('Enter current value of the column {}: '.format(column_name))
    new_value = pyip.inputStr('Enter new value to replace old value: ')
    cust_fields.withColumn(column_name, re.replace(column_name, old_value, new_value)).distinct() \
        .show()


def customer_mod3(ssn=None, mm=None, yy=None):
    sql = "SELECT * FROM cdw_sapp_credit_card "
    data_frame = psql.read_sql(sql, con=connection)
    data_frame["TIMEID"] = pd.to_datetime(data_frame["TIMEID"], format='%Y%m%d')
    spark_df = spark.createDataFrame(data_frame)
    spark_df = spark_df.withColumn("TIMEID", to_date("TIMEID", "yyyy-mm-dd"))
    spark_df = spark_df.withColumn("month", month("TIMEID"))
    spark_df = spark_df.withColumn("year", year("TIMEID"))
    spark_df = spark_df.withColumn("days", dayofmonth("TIMEID"))

    # todo get user input
    print('Module Three:\n\tGenerate a monthly bill for a credit card number for a given month and year')
    cust_value = spark_df.select('CUST_SSN', 'TIMEID', 'TRANSACTION_TYPE',
                                 'TRANSACTION_VALUE').filter(spark_df.CUST_SSN == ssn). \
        filter((spark_df.month == mm) & (spark_df.year == yy))
    cust_value.show()
    print('Credit Card Statement for Month:{} Year:{} '.format(mm, yy))
    cust_value.agg({'TRANSACTION_VALUE': 'sum'}).show()


def customer_mod4():
    sql = "SELECT * FROM cdw_sapp_credit_card "
    data_frame = psql.read_sql(sql, con=connection)
    pd.set_option("display.max_columns", None)

    data_frame["TIMEID"] = pd.to_datetime(data_frame["TIMEID"], format='%Y%m%d')
    spark_df = spark.createDataFrame(data_frame)
    spark_df = spark_df.withColumn("TIMEID", to_date("TIMEID", "yyyy-mm-dd"))
    spark_df = spark_df.withColumn("month", month("TIMEID"))
    spark_df = spark_df.withColumn("year", year("TIMEID"))
    spark_df = spark_df.withColumn("days", dayofmonth("TIMEID"))

    start_date = 2018 - 3 - 11
    end_date = 2018 - 12 - 20
    customer_ssn = 123456698

    cust_value = spark_df.select('TIMEID', 'TRANSACTION_TYPE', 'TRANSACTION_VALUE', 'CUST_SSN') \
        .filter(spark_df.CUST_SSN == customer_ssn) \
        .filter((spark_df.TIMEID == start_date) & (spark_df.TIMEID == end_date)) \
        .orderBy(year(spark_df.TIMEID).desc(), month(spark_df.TIMEID).desc(), dayofmonth(spark_df.TIMEID).desc())
    cust_value.show()


def main():
    while True:
        print("#" * 50)
        print("Customer Transaction Details Menu")
        print("#" * 50)

        print("\n1.Transaction Module")
        print("2.Customer Module")
        print("3.Quit")
        choice = pyip.inputInt("Enter a choice Transaction details or Customer details: ", min=1, max=3)
        if choice == 1:
            while True:
                print("\nSelect an option from the sub_menu.")
                print("1.The transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.")
                print("2.The number and total values of transactions for a given type")
                print("3.The total number and total values of transactions for branches in a given state.")
                print("4.Quit")
                sub_choice = pyip.inputInt("Select an option to check the transaction details: ", min=1, max=4)
                if sub_choice == 1:
                    zipcode = pyip.inputInt("Enter zipcode: ", min=10000, max=99999)
                    mm = pyip.inputInt("Enter Month: ", min=1, max=12)
                    yy = pyip.inputInt("Enter zipcode: ", min=1900, max=2023)
                    transaction_menu_1_1(zipcode=zipcode, mm=mm, yy=yy)
                elif sub_choice == 2:
                    transaction_type = pyip.inputStr("Enter Transaction Type: ")
                    transaction_mod2(transaction_type=transaction_type)
                elif sub_choice == 3:
                    branch_state = pyip.inputStr("Enter Branch State: ")
                    transaction_mod3(branch_state=branch_state)
                else:
                    break
        elif choice == 2:
            while True:
                print("\nSelect an option from the sub_menu.")
                print("1.Used to check the existing account details of a customer.")
                print("2.Used to modify the existing account details of a customer.")
                print("3.Used to generate a monthly bill for a credit card number for a given month and year.")
                print("4.Used to display the transactions made by a customer between two dates. Order by year, month, "
                      "and day in descending order.")
                print("5.Quit")
                sub_choice = pyip.inputInt("Select an option to Get the Customer details: ", min=1, max=5)
                if sub_choice == 1:
                    ssn = pyip.inputStr("Enter Customer SSN: ", max=999999999)
                    customer_mod1(ssn=ssn)
                elif sub_choice == 2:
                    ccn = pyip.inputInt("Enter the 9 digit Customer Credit Card Number: ", max=999999999)
                    customer_mod2(ccn=ccn)
                elif sub_choice == 3:
                    ssn = pyip.inputInt("Enter Customer SSN: ",  max=999999999)
                    mm = pyip.inputInt("Enter Month: ", min=1, max=12)
                    yy = pyip.inputInt("Enter zipcode: ", min=1900, max=2023)
                    customer_mod3(ssn=ssn, mm=mm, yy=yy)
                elif sub_choice == 4:
                    start_date = pyip.inputDate("Enter the start date: ", )
                    customer_mod4()
                else:
                    break

        else:
            break


if __name__ == "__main__":
    # main()
    customer_mod3(123456698,12,2018)


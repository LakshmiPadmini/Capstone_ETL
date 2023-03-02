

import findspark


findspark.init()
import pymysql
import pandas.io.sql as psql
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
import pyinputplus as pyip
from datetime import datetime
import warnings
import secret


# Menu Driven Program For Customer and Transaction Modules.
warnings.filterwarnings('ignore')

pd.set_option("display.max_columns", None)
spark = SparkSession.builder.master("local[1]").appName('Credit_card.com').getOrCreate()

connection = pymysql.connect(
    host='localhost',
    user=secret.user,
    password=secret.password,
    database='creditcard_capstone'
)
cursor = connection.cursor()
# Transaction Modules
def transaction_menu_1_1(zipcode=21042, mm=11, yy=2018):
    sql = "SELECT c.CUST_ZIP,c.SSN,cc.CUST_SSN,cc.TRANSACTION_VALUE,cc.TRANSACTION_TYPE,cc.TIMEID FROM " \
          "cdw_sapp_customer c  JOIN cdw_sapp_credit_card cc ON c.SSN=cc.CUST_SSN"
    # cursor.execute(sql)
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
    # cursor.execute(sql)
    data_frame = psql.read_sql(sql, con=connection)

    transaction_type = data_frame[
        (data_frame['TRANSACTION_TYPE'].str.lower() == transaction_type.lower()) & (data_frame['TRANSACTION_VALUE'])]
    transaction_df = transaction_type.groupby('TRANSACTION_TYPE').agg(count=('TRANSACTION_TYPE', 'count'),
                                                                      value=('TRANSACTION_VALUE', 'sum'))
    transaction_df['value'] = round(transaction_df['value'], 2)
    print(transaction_df)


# transaction_mod2()
def transaction_mod3(branch_state='PA'):
    sql = "SELECT bc.BRANCH_CODE,bc.BRANCH_STATE,cc.TRANSACTION_VALUE,cc.TRANSACTION_TYPE,cc.BRANCH_CODE \
               FROM cdw_sapp_branch bc JOIN cdw_sapp_credit_card cc ON bc.BRANCH_CODE=cc.BRANCH_CODE"
    data_frame = psql.read_sql(sql, con=connection)
    branch_transaction = data_frame[(data_frame['BRANCH_STATE'].str.lower() == branch_state.lower()) & (data_frame['TRANSACTION_VALUE'])]
    trans_by_branch_state = branch_transaction.groupby(['BRANCH_STATE']).agg(
        total_transactions=('TRANSACTION_VALUE', 'sum'))
    print(trans_by_branch_state)


# Customer Modules
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

    spark_df = spark.createDataFrame(data_frame)
    # todo get user input
    print('Modify existing account details of a customer by entering customers credit card number\n')
    cust_fields = spark_df.select('SSN', 'FIRST_NAME', 'LAST_NAME', 'MIDDLE_NAME', 'CUST_COUNTRY', 'CUST_CITY',
                                  'CUST_EMAIL', 'CUST_PHONE', 'FULL_STREET_ADDRESS', 'CUST_STATE', 'CUST_ZIP',
                                  'CREDIT_CARD_NO').filter(spark_df.CREDIT_CARD_NO == ccn)
    cust_fields.distinct().show()
    # todo ask user what to modify
    column_name = pyip.inputStr('Enter NAME of the column to update: ')

    index = cust_fields.columns.index(column_name)
    old_value = cust_fields.collect()[0][index]
    print('Current value of the column {} is {} '.format(column_name, old_value))
    new_value = pyip.inputStr('Enter new value to replace old value: ')
    cust_fields = cust_fields.withColumn(column_name, regexp_replace(column_name, old_value, new_value))
    cust_fields.show()


def customer_mod3(ssn=None, mm=None, yy=None):
    sql = "SELECT * FROM cdw_sapp_credit_card "
    data_frame = psql.read_sql(sql, con=connection)
    data_frame["TIMEID"] = pd.to_datetime(data_frame["TIMEID"], format='%Y%m%d')
    data_frame = data_frame[(data_frame['TIMEID'].dt.month == mm)
                            & (data_frame['TIMEID'].dt.year == yy)
                            & (data_frame["CUST_SSN"] == ssn) ]
    result = data_frame.groupby("CUST_SSN")["TRANSACTION_VALUE"].sum()
    print('Credit Card Statement for Month:{} Year:{} monthly bill {} '.format(mm, yy, round(result[ssn], 2)))


def customer_mod4(customer_ssn=None, start_date=None, end_date=None):
    sql = "SELECT * FROM cdw_sapp_credit_card "
    data_frame = psql.read_sql(sql, con=connection)
    start = datetime.strptime(start_date, "%Y/%m/%d")
    end = datetime.strptime(end_date, "%Y/%m/%d")
    data_frame["TIMEID"] = pd.to_datetime(data_frame["TIMEID"], format='%Y%m%d')

    data_frame = data_frame[ (data_frame['TIMEID'].between(start, end)) & (data_frame['CUST_SSN'] == customer_ssn)]
    print(data_frame[['TRANSACTION_TYPE', 'TRANSACTION_VALUE', 'TIMEID']])


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
                print("1.The transactions made by customers living in a given zip code for"
                      " a given month and year. Order by day in descending order.")
                print("2.The number and total values of transactions for a given type")
                print("3.The total number and total values of transactions for branches"
                      " in a given state.")
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
                print("1.To check the existing account details of a customer.")
                print("2.To modify the existing account details of a customer.")
                print("3.To generate a monthly bill for a credit card number for a given month and year.")
                print("4.To display the transactions made by a customer between two dates. Order by year, month, "
                      "and day in descending order.")
                print("5.Quit")
                sub_choice = pyip.inputInt("Select an option to Get the Customer details: ", min=1, max=5)
                if sub_choice == 1:
                    ssn = pyip.inputInt("Enter Customer SSN(Ex: 123456698): ", max=999999999)
                    customer_mod1(ssn=ssn)
                elif sub_choice == 2:
                    ccn = pyip.inputInt("Enter the 16 digit Customer Credit Card Number(4210653369905302): ")
                    customer_mod2(ccn=ccn)
                elif sub_choice == 3:
                    ssn = pyip.inputInt("Enter Customer SSN (Ex: 123456698): ", max=999999999)
                    mm = pyip.inputInt("Enter Month (Ex:3): ", min=1, max=12)
                    yy = pyip.inputInt("Enter Year (Ex: 2018): ", min=1900, max=2023)
                    customer_mod3(ssn=ssn, mm=mm, yy=yy)
                elif sub_choice == 4:
                    customer_ssn = pyip.inputInt("Enter SSN (Ex: 123456698): ", )
                    start_date = pyip.inputStr("Enter the start date (Ex:2018/3/11): ", )
                    end_date = pyip.inputStr("Enter the end date (Ex:2018/12/20): ", )
                    customer_mod4(customer_ssn=customer_ssn, start_date=start_date, end_date=end_date)
                else:
                    break

        else:
            break


if __name__ == "__main__":
    main()

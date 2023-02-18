import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import pandas.io.sql as psql

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
print(data_frame.head())

cursor.close()
connection.close()

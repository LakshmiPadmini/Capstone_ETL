import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import pandas.io.sql as psql
import seaborn as sns

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
#print(data_frame)

#print(data)

data = len(data_frame)
data1 = data_frame.groupby(by = 'Self_Employed')["Application_Status"].value_counts()


self_emp_approved = data_frame.loc[(data_frame['Self_Employed'] == 'Yes') & (data_frame['Application_Status'] == 'Y'), 'Application_Status'].count()
print(self_emp_approved)
percentage = self_emp_approved/data*100
print(percentage)
sns_plot = sns.barplot(x="Self_Employed", y="Application_Status", data=data1, estimator=percentage)
sns_plot.barh
import findspark
findspark.init()

import pymysql
import matplotlib.pyplot as plt
import pandas.io.sql as psql
import seaborn as sns
import pyinputplus as pyip
import warnings

warnings.filterwarnings('ignore')
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='password',
    database='creditcard_capstone'
)
###Data Visualization plots for Loan Application Data
def get_loan_api_df():
    sql = "SELECT * FROM CDW_SAPP_loan_application"
    return psql.read_sql(sql, con=connection)

###Find Percentage of applications approved for self-employed applicants
def plot_self_employed(df):
    data_api = df.groupby(['Self_Employed', 'Application_Status'])['Application_ID'].count()
    data_api = data_api.reset_index()
    total_applications = len(df)
    #data_api = data_api[(data_api['Application_Status'] == 'Y') & (data_api['Self_Employed'] == 'Yes')]
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
    plt.show()
###Find the percentage of rejection for married male applicants.
def plot_married_applicants(df):

    df_maarried_male = df.groupby(["Married","Application_Status","Gender"])["Application_ID"].count()
    df_maarried_male = df_maarried_male.reset_index()
    print(df_maarried_male)
    total_applications = len(df)
    #df_maarried_male = df_maarried_male[(df_maarried_male['Married'] == 'Yes') & (df_maarried_male['Application_Status'] == 'N' ) & (df_maarried_male['Gender'] == 'Male')]
    df_maarried_male['Count_Percent'] = df_maarried_male['Application_ID']/total_applications * 100
    #df_maarried_male
    sns.set(rc={"figure.figsize":(4,6)})
    sns.set_theme(style="whitegrid", palette="hls")
    sns.barplot(x ='Married',
            y='Count_Percent',
            hue='Gender',
            data=df_maarried_male).set(title ="Percentage of rejection for married male applicants.")
    plt.show()

def main():
    loan_API_df = get_loan_api_df()

    while True:
        print("#" * 50)
        print("Loan API Data Visualizations")
        print("#" * 50)
        print("1. percentage of applications approved for self-employed applicants")
        print("2. percentage of rejection for married male applicants")
        print("3. Quit")
        choice = pyip.inputInt("Enter a Menu Choice: ", min=1, max=6)
        if choice == 1:
            plot_self_employed(loan_API_df)
        elif choice == 2:
            plot_married_applicants(loan_API_df)
        else:
            break

if __name__ == "__main__":
    main()



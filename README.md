# Capstone: Retrieving, Analyzing, and Visualizing Data with Python
## Introduction
This capstone project is to manage an ETL process for a Loan Application dataset and 
a Credit Card dataset. 

Credit Card System: The Credit Card System database is an independent system developed for managing activities such as 
registering new customers and approving or canceling requests, etc., using the architecture.

Loan Application: Banks deal in all home loans. They have a presence across all urban, semi-urban,and rural areas.
Customers first apply for a home loan; after that, a company will validate the customer's eligibility
for a loan.

 ![img_1.png](docs/img_1.png)

### Virtual Environment Setup:
* python -m venv venv
* pip list 

To activate the environment
* venv\Scripts\activate.bat
### Requirements
Top-level requirements are stored in requirements.txt.
* pip freeze > requirements.txt
to freeze all sub dependencies into requirements.txt.

### Installation Guide
Necessary dependencies before running this application: 
* pip install findspark
* pip install pandas
* pip install matplotlib
* pip install pyspark
* pip install seaborn
* pip install regex
* pip install numpy
* pip install pyinputplus
* pip install datetime
* pip install pymysql
* pip install requests

## Technologies
Language: Python 3.9.12
Database:MariaDB and HeidiSQL for GUI
Libraries used in python:
Pandas
Matplotlib
Seaborn
requests
Apache Spark(Spark Core,Spark Sql)

## Business Requirements - ETL
  A credit card is issued to users to enact the payment system. It allows the 
cardholder to access financial services in exchange for the holder's promise to pay 
for them later. Below are three files that contain the customer’s transaction information 
and inventories in the credit card information.

CDW_SAPP_CUSTOMER.JSON: This file has the existing customer details.
CDW_SAPP_CREDITCARD.JSON: This file contains all credit card transaction information.
CDW_SAPP_BRANCH.JSON: Each branch’s information and details are recorded in this file.

## Load Credit Card Database (SQL)
mysql << create database creditcard_capstone
### Python and Pyspark Program to load/write the “Credit Card System Data” into RDBMS(creditcard_capstone).

1. customer_data.py - Extract,Transform,Load OF Customer Json Format Data Into
                     MySql Database CreditCard_Capstone.

2.credit_data.py - Extract,Transform,Load OF Credit_Card Json Format Data Into
                     MySql Database CreditCard_Capstone.

3.branch_data.py - Extract,Transform,Load OF branch_data Json Format Data Into
                     MySql Database CreditCard_Capstone.

4.main_etl.py -

CDW_SAPP_CUSTOMER: Table with existing customer details.
CDW_SAPP_CREDITCARD: Table contains all credit card transaction information.
CDW_SAPP_BRANCH: Each branch’s information and details are recorded in this Table.

![Screenshot 2023-02-27 104449.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20104449.png)


## Application Front-End
## Console-based Python program to satisfy System Requirements for Transaction and Customer
Details Modules.

1.The total number and total values of transactions for branches in a given state.
![Screenshot 2023-02-27 111149.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20111149.png)

2.To modify the existing account details of a customer
![Screenshot 2023-02-27 111618.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20111618.png)
![Screenshot 2023-02-27 111709.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20111709.png)


## Data analysis and Visualization
To analyze and visualize the data according to the requirements.

1.Find and plot which transaction type has a high rate of transactions.
![Screenshot 2023-02-27 112819.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20112819.png)
2.Find and plot which state has a high number of customers.
![Screenshot 2023-02-27 112854.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20112854.png)
3.Find and plot the sum of all transactions for the top 10 customers, and which
customer has the highest transaction amount.hint(use CUST_SSN). 

4.Find and plot the top three months with the largest transaction data.
![Screenshot 2023-02-27 112655.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20112655.png)
5.Find and plot which branch processed the highest total dollar value of healthcare 
transactions.
![Screenshot 2023-02-27 112742.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20112742.png)


## LOAN application Data API

Banks want to automate the loan eligibility process (in real-time) based on customer details provided while
filling out the online application form. These details are Gender, Marital Status, Education, 
Number of Dependents, Income, Loan Amount, Credit History, and others. To automate this process, 
they have the task of identifying the customer segments to those who are eligible for loan amounts 
so that they can specifically target these customers. Here they have provided a partial dataset.

API Endpoint: https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json
#Python and Pyspark Program to load/write the “Loan Application Data” into 
RDBMS(creditcard_capstone).
1. Loan_API.py - Python program to GET (consume) data from the above API endpoint for the loan application 
                 dataset with the status code 200.

CDW_SAPP_loan_application: Table with  customer home loans details.
#Data analysis and Visualization
1.Find and plot the percentage of applications approved for self-employed applicants.
![Screenshot 2023-02-27 131015.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20131015.png)
2.Find the percentage of rejection for married male applicants.

![Screenshot 2023-02-27 131106.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20131106.png)
![Screenshot 2023-02-27 131146.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20131146.png)

## Tableau
## Loan Application Analysis using Tableau

![Screenshot 2023-02-27 132614.png](..%2FDesktop%2FNew%20folder%2FScreenshot%202023-02-27%20132614.png)



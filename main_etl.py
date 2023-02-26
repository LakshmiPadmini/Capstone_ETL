from Customer_Credit_Card_App.branch_data import BranchData
from Customer_Credit_Card_App.credit_data import CreditData
from Customer_Credit_Card_App.customer_data import CustomerData
import warnings

warnings.filterwarnings('ignore')
#
#
#
customer_path = "./dataset/creditcard_dataset/cdw_sapp_custmer.json"
credit_path = "./dataset/creditcard_dataset/cdw_sapp_credit.json"
branch_path = "./dataset/creditcard_dataset/cdw_sapp_branch.json"

credit_data = CreditData()
credit_df = credit_data.load_data(path=credit_path, write_to_db=True, mode='overwrite')
credit_df.show()

customer_data = CustomerData()
customer_df = customer_data.load_data(path=customer_path, write_to_db=True, mode='overwrite')
customer_df.show()

branch_data = BranchData()
branch_df = branch_data.load_data(path=branch_path, write_to_db=True, mode='overwrite')
branch_df.show()












from src.branch_data import BranchData
from src.credit_data import CreditData
from src.customer_data import CustomerData

branch_data = BranchData(path='../dataset/creditcard_dataset/cdw_sapp_branch.json')
branch_data.load_data()

credit_data = CreditData(path='../dataset/creditcard_dataset/cdw_sapp_credit.json')
credit_data.load_data()
customer_data = CustomerData(path='../dataset/creditcard_dataset/cdw_sapp_custmer.json')
customer_data.load_data()

df = branch_data.get_data()







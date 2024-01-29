from lib import ConfigReader

def read_raw_data(spark,env):
    conf=ConfigReader.get_app_config(env)
    rawdata_file_path=conf["rawdata.file.path"]
    return spark.read\
        .format("csv")\
        .option("header","true")\
        .option("InferSchema","true") \
        .load(rawdata_file_path)
        
def get_customers_schema():
    customer_schema = 'member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, addr_state string, zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float, verification_status_joint string'    
    return customer_schema
#creatingcustomersdataframe
def read_customers_raw(spark,env):
    conf=ConfigReader.get_app_config(env)
    customers_file_path=conf["customers_raw.file.path"]
    return spark.read\
        .format("csv")\
        .option("header","true")\
        .schema(get_customers_schema())\
        .load(customers_file_path)


def get_loans_schema():
    loans_schema = 'loan_id string, member_id string, loan_amount float, funded_amount float, loan_term_months string, interest_rate float, monthly_installment float, issue_date string, loan_status string, loan_purpose string, loan_title string'    
    return loans_schema
#creatingcustomersdataframe
def read_loans_raw(spark,env):
    conf=ConfigReader.get_app_config(env)
    loans_file_path=conf["loans_raw.file.path"]
    return spark.read\
        .format("csv")\
        .option("header","true")\
        .schema(get_loans_schema())\
        .load(loans_file_path)
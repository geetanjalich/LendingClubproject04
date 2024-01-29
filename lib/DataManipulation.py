from pyspark.sql.functions import * 
def generating_member_id(raw_df):
    return raw_df.withColumn("name_sha2",sha2(concat_ws('||',*['emp_title','emp_length','home_ownership','annual_inc','zip_code','addr_state','grade','sub_grade','verification_status']),256))
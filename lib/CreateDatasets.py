from pyspark.sql.functions import * 
from lib import ConfigReader

def create_datasets(spark,new_raw_df_outliers_removal,env):
    conf=ConfigReader.get_app_config(env)
    customers_rawdata_file_path=conf["customers_raw.file.path"]
    loans_rawdata_file_path=conf["loans_raw.file.path"]
    loans_defaulters_rawdata_file_path=conf["loans_defaulters_raw.file.path"]
    loans_defaulters_publicrec_rawdata_file_path=conf["loans_defaulters_publicrec_raw.file.path"]

    # Generate a member id using standard hash function sha
    new_raw_df_outliers_removal.createOrReplaceTempView("raw_data")

    spark.sql("""select name_sha2 as member_id,emp_title,emp_length,home_ownership,annual_inc,addr_state,zip_code,'USA' as country,grade,sub_grade,
    verification_status,tot_hi_cred_lim,application_type,annual_inc_joint,verification_status_joint from raw_data
    """).repartition(1).write \
        .option("header","true")\
        .format("csv") \
        .mode("overwrite") \
        .option("path",customers_rawdata_file_path) \
        .save()
    
    spark.sql("""select id as loan_id, name_sha2 as member_id,loan_amnt,funded_amnt,term,int_rate,installment,issue_d,loan_status,purpose,
        title from raw_data""") \
        .repartition(1) \
        .write \
        .format('csv') \
        .option('header','true') \
        .mode('overwrite') \
        .option("path",loans_rawdata_file_path) \
                .save()
    
    spark.sql("""select id as loan_id,total_rec_prncp,total_rec_int,total_rec_late_fee,total_pymnt,last_pymnt_amnt,last_pymnt_d,next_pymnt_d from raw_data""") \
        .repartition(1) \
        .write \
        .option("header",True)\
        .format("csv") \
        .mode("overwrite") \
        .option("path",loans_defaulters_rawdata_file_path) \
        .save()
    
    spark.sql("""select name_sha2 as member_id,delinq_2yrs,delinq_amnt,pub_rec,pub_rec_bankruptcies,inq_last_6mths,total_rec_late_fee,mths_since_last_delinq,mths_since_last_record from raw_data""").repartition(1).write \
        .option("header",True)\
        .format("csv") \
        .mode("overwrite") \
        .option("path",loans_defaulters_publicrec_rawdata_file_path) \
        .save()
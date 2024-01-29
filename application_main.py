import sys 
from lib import DataCleaning,DataManipulation,DataReader,Utils,CreateDatasets
from pyspark.sql.functions import * 
if __name__=='__main__':
    if len(sys.argv)<2:
        print("Please specify the environment")
        sys.exit(-1)
    job_run_env=sys.argv[1]

    print("Creating Spark Session")
    spark=Utils.get_spark_session(job_run_env)
    print("Created Spark Session")
    #raw_df=DataReader.read_raw_data(spark,job_run_env)

    # GEnerate a member id using standard hash function sha
    #new_raw_df = DataManipulation.generating_member_id(raw_df)

    #Removal of outliers
    #new_raw_df_outliers_removal =DataCleaning.clean_raw_df_with_outliers(spark,new_raw_df)

    #CreateDatasets.create_raw_datasets(spark,new_raw_df_outliers_removal,job_run_env)

    #customers_raw_df=DataReader.read_customersraw(spark,job_run_env)
   # DataCleaning.data_cleaning_customers(spark,customers_raw_df,job_run_env)
    
    #loans_raw_df=DataReader.read_loansraw(spark,job_run_env)
   #DataCleaning.data_cleaning_loans(spark,loans_raw_df,job_run_env)

    loansrepayments_raw_df=DataReader.read_loansrepayments_raw(spark,job_run_env)
    DataCleaning.data_cleaning_loanrepaymensts(spark,loansrepayments_raw_df,job_run_env)

    #loansdefaulters_raw_df=DataReader.read_loandefaulterssraw(spark,job_run_env)
    #DataCleaning.data_cleaning_loans(spark,loansrepayments_raw_df,job_run_env)

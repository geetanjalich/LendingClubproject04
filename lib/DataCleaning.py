from pyspark.sql.functions import *
from lib import ConfigReader 
def clean_raw_df_with_outliers(spark,new_raw_df):

    new_raw_df.createOrReplaceTempView("new_raw_table")
    raw_remove_outlier_df = spark.sql("select * from new_raw_table where name_sha2 not like 'e3b0c44298fc1c149%'")
    return raw_remove_outlier_df

def clean_customers_raw(spark,customers_raw_df,env):
    conf=ConfigReader.get_app_config(env)
    customers_file_path=conf["customers_clean.file.path"]
     #renmaing columns
    customers_raw_df.printSchema()
    customer_df_renamed = customers_raw_df.withColumnRenamed("annual_inc", "annual_income") \
    .withColumnRenamed("addr_state", "address_state") \
    .withColumnRenamed("zip_code", "address_zipcode") \
    .withColumnRenamed("country", "address_country") \
    .withColumnRenamed("tot_hi_credit_lim", "total_high_credit_limit") \
    .withColumnRenamed("annual_inc_joint", "join_annual_income")
    
    #ingesting date to the a new column
    customers_df_ingestd = customer_df_renamed.withColumn("ingest_date", current_timestamp())
    
    #Remove duplicate rows
    customers_distinct = customers_df_ingestd.distinct()
    customers_distinct.createOrReplaceTempView("customers")
    #. Remove the rows where annual_income is null
    customers_income_filtered = spark.sql("select * from customers where annual_income is not null")
    customers_income_filtered.createOrReplaceTempView("customers")

    #convert emp_length to integer
    customers_emplength_cleaned = customers_income_filtered.withColumn("emp_length", regexp_replace(col("emp_length"), "(\D)",""))    
    customers_emplength_casted = customers_emplength_cleaned.withColumn("emp_length", customers_emplength_cleaned.emp_length.cast('integer'))
   
    #we need to replace all the nulls in emp_length column with average of this column
    customers_emplength_casted.filter("emp_length is null").count()
    customers_emplength_casted.createOrReplaceTempView("customers")
    avg_emp_length = spark.sql("select floor(avg(emp_length)) as avg_emp_length from customers").collect()
    avg_emp_duration = avg_emp_length[0][0]
    customers_emplength_replaced = customers_emplength_casted.na.fill(avg_emp_duration, subset=['emp_length'])
    customers_emplength_replaced.createOrReplaceTempView("customers")

    #Clean the address_state(it should be 2 characters only),replace all others with NA
    customers_state_cleaned = customers_emplength_replaced.withColumn(
    "address_state",
    when(length(col("address_state"))> 2, "NA").otherwise(col("address_state"))
)
    customers_state_cleaned.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", customers_file_path) \
    .save()

def clean_loans_raw(spark,loans_raw_df,env):
    conf=ConfigReader.get_app_config(env)
    loans_file_path=conf["loans_clean.file.path"]
     #renmaing columns
    
    loans_raw_df.printSchema()
    loans_df_ingestd = loans_raw_df.withColumn("ingest_date", current_timestamp())
    loans_df_ingestd.createOrReplaceTempView("loans")

    #Dropping the rows which has null values in the mentioned columns
    columns_to_check = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]
    loans_filtered_df = loans_df_ingestd.na.drop(subset=columns_to_check)
    loans_filtered_df.createOrReplaceTempView("loans")

    #convert loan_term_months to integer
    loans_term_modified_df = loans_filtered_df.withColumn("loan_term_months", (regexp_replace(col("loan_term_months"), " months", "") \
    .cast("int") / 12) \
    .cast("int")) \
    .withColumnRenamed("loan_term_months","loan_term_years")

    # clean the loans_purpose column

    loans_term_modified_df.createOrReplaceTempView("loans")
    spark.sql("select loan_purpose, count(*) as total from loans group by loan_purpose order by total desc")
    loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical", "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"]
    loans_purpose_modified = loans_term_modified_df.withColumn("loan_purpose", when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose")).otherwise("other"))
    loans_purpose_modified.createOrReplaceTempView("loans")
    loans_purpose_modified.groupBy("loan_purpose").agg(count("*").alias("total")).orderBy(col("total").desc())

    loans_purpose_modified.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", loans_file_path) \
    .save()

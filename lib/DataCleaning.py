from pyspark.sql.functions import * 
def clean_raw_df_with_outliers(spark,new_raw_df):

    new_raw_df.createOrReplaceTempView("new_raw_table")
    raw_remove_outlier_df = spark.sql("select * from new_raw_table where name_sha2 not like 'e3b0c44298fc1c149%'")
    return raw_remove_outlier_df




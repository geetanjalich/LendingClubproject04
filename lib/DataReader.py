from lib import ConfigReader

def read_raw_data(spark,env):
    conf=ConfigReader.get_app_config(env)
    rawdata_file_path=conf["rawdata.file.path"]
    return spark.read\
        .format("csv")\
        .option("header","true")\
        .option("InferSchema","true") \
        .load(rawdata_file_path)
        

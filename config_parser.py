from pyspark.sql import SparkSession
import configparser
if __name__=="__main__":
    spark=SparkSession.builder.appName("read using config").master("local").getOrCreate()
    sc=spark.sparkContext
    sc.setLogLevel("ERROR")
    config_read=configparser.ConfigParser()
    config_read.read(r'C:\Users\mishr\PycharmProjects\pythonProject7\config\config.ini')
    input_path=config_read.get('INPUTPATH','input_file')
    print(input_path)
    df_json=spark.read.json(input_path)
    df_json.show(truncate=False)
    df_json.printSchema()
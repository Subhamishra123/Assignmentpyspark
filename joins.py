from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import broadcast

if __name__=="__main__":
    spark=SparkSession.builder.appName("joins").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df_emp=spark.read.format("csv").\
                option("delimiter",",").\
                option("inferschema",True).\
                load("file:////C://Users//mishr//Downloads//emp_data.csv").\
                toDF("emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary")
    df_emp.show()
    df_dept=spark.read.format("csv").option("delimiter",",").\
                        option("inferschema",True).\
                        load("file:////C://Users//mishr/Downloads//dept_data.csv").\
                        toDF("dept_name","dept_id")
    df_dept.show()
    print("inner join")
    df_emp.join(df_dept,df_emp['emp_dept_id']==df_dept['dept_id'],'inner').show()
    print("left outer join")
    df_emp.join(df_dept,df_emp['emp_dept_id']==df_dept['dept_id'],'left').show()
    print("right outrt join")
    df_emp.join(df_dept,df_emp['emp_dept_id']==df_dept['dept_id'],'right').show()
    print("left semi join")
    df_emp.join(df_dept,df_emp['emp_dept_id']==df_dept['dept_id'],'leftsemi').show()
    print("left anti join")
    df_emp.join(df_dept,df_emp['emp_dept_id']==df_dept['dept_id'],'leftanti').show()
    print("other use case")
    df_dept.join(df_emp,df_emp['emp_dept_id']==df_dept['dept_id'],'leftanti').show()
    print("outer join")
    df_emp.join(df_dept,df_emp['emp_dept_id']==df_dept['dept_id'],'outer').show()
    print("cross join")
    df_emp.crossJoin(df_dept).show()
    print("broadcast join")
    df_emp.join(broadcast(df_dept),df_emp['emp_dept_id']==df_dept['dept_id']).show()


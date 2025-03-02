from pyspark.sql import SparkSession

if __name__=="__main__":
    spark=SparkSession.builder.appName("handling null").master("local").getOrCreate()
    sc=spark.sparkContext
    sc.setLogLevel("ERROR")
    df_emp_uncleans=spark.read.format("csv").option("delimiter",',').\
                            option("inferschema",True).\
                            option("header",True).\
                            load("file:///C://Users//mishr//Downloads//emp_uncleans.csv")

    df_emp_uncleans.show()

    #drop null values
    print("drop null values")
    null_df=df_emp_uncleans.na.drop()
    null_df.show()

    #drop only rows which has null in the joining year
    print("drop only rows which has null in the joining year")
    null_joining_year=df_emp_uncleans.na.drop(subset=['Joining Year'])
    null_joining_year.show()

    # drop only rows which has null in the joining year and department
    print("drop only rows which has null in the joining year and department")
    null_joining_year_dept = df_emp_uncleans.na.drop(subset=['Joining Year', 'Department'])
    null_joining_year_dept.show()

    #replace null value with pyspark in IT department
    print("replace null value with pyspark in IT department")
    null_replace=df_emp_uncleans.na.fill('Pyspark',subset=['Department'])
    null_replace.show()

    #in a dept if we have IT column replace it with Bigdata
    print("replace null value with pyspark in IT department")
    it_dept_replace=df_emp_uncleans.replace({'IT':'Bigdata'},subset=['Department'])
    it_dept_replace.show()

    #check whether first name and last name is not null
    print('check whether first name and last name is not null')
    df_name_check=df_emp_uncleans.filter(df_emp_uncleans['First Name'].isNotNull() & df_emp_uncleans['Last Name'].isNotNull())
    df_name_check.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct,sum, max, rank, monotonically_increasing_id
from pyspark.sql.window import Window



def create_spark_obj():
    spark=SparkSession.builder.appName("spark-assignment").master("local").getOrCreate()
    sparkcontext=spark.sparkContext
    return spark

if __name__=="__main__":
    spark=create_spark_obj()
    # create sales data
    sales_data = [
        ('A', '2021-01-01', '1'),
        ('A', '2021-01-01', '2'),
        ('A', '2021-01-07', '2'),
        ('A', '2021-01-10', '3'),
        ('A', '2021-01-11', '3'),
        ('A', '2021-01-11', '3'),
        ('B', '2021-01-01', '2'),
        ('B', '2021-01-02', '2'),
        ('B', '2021-01-04', '1'),
        ('B', '2021-01-11', '1'),
        ('B', '2021-01-16', '3'),
        ('B', '2021-02-01', '3'),
        ('C', '2021-01-01', '3'),
        ('C', '2021-01-01', '1'),
        ('C', '2021-01-07', '3')
    ]
    sales_cols = ["customer_id", "order_date", "product_id"]
    sales_df=spark.createDataFrame(sales_data,sales_cols)
    #sales_df.show()
    # menu data
    menu_data = [('1', 'palak_paneer', 100),
                 ('2', 'chicken_tikka', 150),
                 ('3', 'jeera_rice', 120),
                 ('4', 'kheer', 110),
                 ('5', 'vada_pav', 80),
                 ('6', 'paneer_tikka', 180)
                 ]
    # cols
    menu_cols = ['product_id', 'product_name', 'price']
    menu_df=spark.createDataFrame(menu_data,menu_cols)
    #menu_df.show()
    # create member data
    members_data = [('A', '2021-01-07'),
                    ('B', '2021-01-09')]
    # cols
    members_cols = ["customer_id", "join_date"]
    members_df=spark.createDataFrame(members_data,members_cols)
    #members_df.show()
    df_right=sales_df.join(menu_df,on=(sales_df['product_id'] == menu_df['product_id']),how='right')
    #df_right.show()
    #df_right.groupby()
    df_right.filter("customer_id is not null").\
        groupby("customer_id").sum("price").\
        alias("amount_spend").orderBy("customer_id").show()
    sales_df.show()
    sales_df.groupBy("customer_id").\
        agg(countDistinct("order_date").\
            alias("no_of_days")).\
        orderBy("customer_id").show()
    sales_window=Window.partitionBy("customer_id").orderBy("order_date")
    sales_df_unique_column=sales_df.withColumn("unique_id",rank().over(sales_window)).orderBy("customer_id")
    sales_df_unique_column.show()
    sales_df_unique_column=sales_df_unique_column.withColumnRenamed("product_id","product_id1")
    right_join=sales_df_unique_column.join(menu_df,on=(menu_df['product_id']==sales_df_unique_column['product_id1']),how='right')
    right_join = right_join.filter("customer_id is not null").orderBy("customer_id")
    right_join.show()
    right_join.filter("unique_id=1").select("customer_id","order_date","product_id","product_name").show()
    #Find out the most purchased item from the menu and how many times the customers purchased it.
    print("new problem")
    sales_df=sales_df.withColumnRenamed("product_id","product_id1")
    join2=sales_df.join(menu_df,on=(menu_df['product_id']==sales_df['product_id1']),how='right')
    join2=join2.filter('customer_id is not null').drop("product_id1")
    join2.show()
    join2=join2.groupBy("customer_id","product_id").agg(count("*").alias("count")).orderBy("customer_id")
    join2=join2.groupby("product_id").agg(sum("count").alias("count")).orderBy("product_id")
    join2=join2.withColumnRenamed("product_id","product_id1")
    join3=join2.join(menu_df,on=(menu_df['product_id']==join2['product_id1']),how='right').\
        filter('product_id is not null').\
        filter('count is not null').\
        orderBy("product_id").\
        drop("product_id1")
    join3.show()
    max_count=join3.select(max("count")).collect()[0][0]
    join3.filter(join3['count']==max_count).select('product_name','count').show()
    print("================================")
    sales_df.show()
    menu_df.show()
    sales_df.groupBy("customer_id","product_id1").agg(count("product_id1")).orderBy("customer_id").show()
    #5) Which item was the most popular for each customer?
    """
    sales_df=sales_df.withColumn("unique_id",monotonically_increasing_id())
    sales_df.show()
    #customer_window=Window.partitionBy("customer_id").orderBy("order_date")
    customer_window = Window.partitionBy("customer_id").orderBy("order_date", "unique_id")

    erank=sales_df.withColumn("rownum",row_number().over(customer_window)).drop("unique_id").orderBy("customer_id")
    erank.filter("rownum=1").drop("rownum").show()
    erank.join(menu_df,on=(menu_df["product_id"]==erank["product_id"]),how="right").filter("customer_id is not null").show()

    
    finaldf=drank.filter("drank=1").orderBy("customer_id").drop("drank")
    finaldf.show()
    finaldf.join(menu_df,on=(menu_df["product_id"]==finaldf["product_id"]),how="right").\
        filter("customer_id is not null").\
        orderBy("customer_id").\
        show()
    """


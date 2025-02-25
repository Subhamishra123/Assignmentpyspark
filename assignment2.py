from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, rank, monotonically_increasing_id, countDistinct
from pyspark.sql.window import Window


def create_spark_obj():
    spark=SparkSession.builder.appName("spark-assignment").master("local").getOrCreate()
    sparkcontext=spark.sparkContext
    sparkcontext.setLogLevel("ERROR")
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


    sales_df.show()
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
    menu_df.show()

    members_data = [('A', '2021-01-07'),
                    ('B', '2021-01-09')]
    members_cols = ["customer_id1", "join_date"]
    members_df = spark.createDataFrame(members_data, members_cols)
    members_df.show()
    join_sales_cols_members_cols=sales_df.join(members_df,
                                               on=((sales_df['customer_id']==members_df['customer_id1']) &
                                                   (sales_df['order_date']>=members_df['join_date'])
                                               )).orderBy('customer_id').drop("customer_id1","join_date")
    join_window=Window.partitionBy("customer_id").orderBy("order_date")
    join_window_unique=join_sales_cols_members_cols.withColumn("unique_id",rank().over(join_window))
    join_window_unique.show()
    join_sales_cols_members_cols.show()

    join_window_unique=join_window_unique.filter("unique_id=1").drop("order_date","unique_id")
    join_window_unique.show()
    join_window_unique.join(menu_df,on=(menu_df['product_id']==join_window_unique['product_id'])).drop("price","product_id").show()

    #7) Which item was purchased before the customer became a member?

    print("========================")
    sales_df.show()
    members_df.show()

    join_sales_members=sales_df.join(members_df,
                                     on=((sales_df['customer_id']==members_df['customer_id1'])&
                                         (sales_df['order_date']<members_df['join_date'])
                                     )).orderBy("customer_id").drop("customer_id1","join_date")
    join_sales_members.show()

    window_sales_members=Window.partitionBy("customer_id").orderBy(join_sales_members["order_date"].desc())
    #join_window_unique=join_sales_cols_members_cols.withColumn("unique_id",rank().over(join_window))
    join_check=join_sales_members.withColumn("unique_id",rank().over(window_sales_members))
    join_check = join_check.filter('unique_id=1').drop("unique_id")
    join_check.join(menu_df,on=(menu_df['product_id']==join_check['product_id'])).drop("price","product_id").orderBy(col("customer_id").desc()).show()

    #Question 08:- What is the total items and amount spent for each member before they became a member?
    print("===================")
    sales_df.show()
    menu_df.show()
    menu_df=menu_df.withColumnRenamed("product_id","product_id1")
    #members_df=members_df.withColumnRenamed("customer_id1","customer_id")
    members_df.show()
    join_data=members_df.join(sales_df,
                              on=((members_df['customer_id1']==sales_df['customer_id']))&
                                 (sales_df['order_date']<members_df['join_date'])
                              ).drop("join_date","customer_id1").orderBy("customer_id")
    join_data=join_data.join(menu_df,on=(join_data['product_id']==menu_df['product_id1']))
    join_data = join_data.groupBy("customer_id").agg(sum("price").alias("amount_spent"),countDistinct("product_id").alias("total_items")).orderBy("customer_id")
    join_data.show()

    #9) If each rupee spent equates to 10 points and item ‘jeera_rice’ has a 2x points multiplier, find out how many points each customer would have.
    print("==============")
    menu_df.show()
    menu_df=menu_df.withColumnRenamed("product_id1","product_id")
    menu_df=menu_df.withColumn("new_price",when(menu_df['product_id']==3,menu_df['price']*20).otherwise(menu_df['price']*10))
    menu_df.show()
    #new_df=menu_df.join()
    sales_df=sales_df.withColumnRenamed('product_id','product_id1')
    sales_df.show()
    print("==================")
    nes_df=sales_df.join(menu_df,on=(menu_df['product_id']==sales_df['product_id1']),how='inner').drop('product_id1')
    nes_df.show()
    nes_df.groupBy('customer_id').agg(sum('new_price').alias("rewards_points")).drop('order_date','product_name','price','product_id').orderBy("customer_id").show()
    #10) Create the complete table with all data and columns like customer_id, order_date, product_name, price, and member(Y/N).
    print("=====================")
    sales_df.show()
    menu_df.show()
    members_df.show()

    #11) Create the complete table with all data and columns like customer_id, order_date, product_name, price, and member(Y/N).
    full_table=sales_df.join(menu_df,on=(sales_df['product_id1']==menu_df['product_id']),how='inner')
    full_table=full_table.drop('product_id1','new_price')
    print('+++++++++++')
    pivot_table=full_table.join(members_df,
                    on=(members_df['customer_id1']==full_table['customer_id']),how='full')
    print("pivot_table")
    pivot_table.show()
    pivot_table.\
        withColumn("is_Member",
                   when(pivot_table['order_date']>=pivot_table['join_date'],'yes').\
                       when(pivot_table['order_date']<pivot_table['join_date'],'no').\
                   otherwise('no')).\
                    drop('customer_id1','product_id','join_date').\
                    orderBy('customer_id','order_date').\
                    show()

    #12Question 11:- We also require further information about the ranking of customer products.
    # The owner does not need the ranking for non-member purchases,
    # so he expects null ranking values for the records when customers still need to be part of
    # the membership program.
    print("======================")
    sales_df.show()
    menu_df.show()
    members_df.show()
    ranking_final_df = sales_df.join(menu_df, on=(sales_df['product_id1']==menu_df['product_id']), how='left')
    ranking_final_df=ranking_final_df.join(members_df, on=(members_df['customer_id1']==ranking_final_df['customer_id']), how='left')
    ranking_final_df.withColumn('is_member', when(col('order_date') < col('join_date'), 'N')
                .when(col('order_date') >= col('join_date'), 'Y')
                .otherwise('N')).\
    withColumn('rank', when(col('is_member') == 'N', None)
                .when(col('is_member') == 'Y', rank().over(Window.partitionBy('customer_id', 'is_member')
                                                           .orderBy('order_date'))).\
                otherwise(0)).show()




from pyspark.sql import SparkSession
from pyspark.sql import Window,functions as sf

if __name__=="__main__":
    spark=SparkSession.builder.appName("calculate_running_total").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sales_data=[
        ("2024-02-20", "Store A", "Apples", "Fruits", 100, 30),
        ("2024-02-21", "Store A", "Apples", "Fruits", 150, 35),
        ("2024-02-22", "Store A", "Apples", "Fruits", 120, 25),
        ("2024-02-20", "Store A", "Bananas", "Fruits", 200, 40),
        ("2024-02-21", "Store A", "Bananas", "Fruits", 180, 38),
        ("2024-02-22", "Store A", "Bananas", "Fruits", 220, 42),
        ("2024-02-20", "Store B", "Oranges", "Fruits", 130, 28),
        ("2024-02-21", "Store B", "Oranges", "Fruits", 160, 30),
        ("2024-02-22", "Store B", "Oranges", "Fruits", 140, 27),
        ("2024-02-20", "Store B", "Milk", "Dairy", 250, 50),
        ("2024-02-21", "Store B", "Milk", "Dairy", 300, 55),
        ("2024-02-22", "Store B", "Milk", "Dairy", 280, 53),
        ("2024-02-20", "Store C", "Bread", "Bakery", 180, 40),
        ("2024-02-21", "Store C", "Bread", "Bakery", 190, 42),
        ("2024-02-22", "Store C", "Bread", "Bakery", 200, 45),
    ]
    sales_data_cols=['Date','Store','Product','Category','Sales_Amount','Customer_Count']
    df=spark.createDataFrame(sales_data,sales_data_cols)
    df.show()
    window_running_total=Window.partitionBy('Store').orderBy('Date').\
                            rowsBetween(Window.unboundedPreceding,Window.currentRow)
    df_running_total=df.withColumn("Running_Total",sf.sum("Sales_Amount").over(window_running_total))
    df_running_total.show()
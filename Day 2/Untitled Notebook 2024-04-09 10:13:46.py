# Databricks notebook source
# MAGIC %run /Workspace/Users/navallyemul@gmail.com/Spark/includes

# COMMAND ----------

df=spark.read.csv(f"{input_path}/Baby_Names.csv",header=True,inferSchema=True)

# COMMAND ----------

(
spark
 .read
 .csv(f"{input_path}/Baby_Names.csv",header=True,inferSchema=True)
 .write
.option("delta.columnMapping.mode","name")
 .saveAsTable("santosh.baby_name_broze")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from santosh.baby_name_broze

# COMMAND ----------

df=spark.read.table("santosh.baby_name_broze")

# COMMAND ----------

df.show()

# COMMAND ----------

df.filter("Year in(2007,2008) and Sex= 'M'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#sort-functions

# COMMAND ----------

df.orderBy(col("Year").desc(),col("First Name").desc()).display()

# COMMAND ----------

emp = [(1, "AAA", "dept1", 1000, "2019-02-01 15:12:13"),
    (2, "BBB", "dept1", 1100, "2018-04-01 5:12:3"),
    (3, "CCC", "dept1", 3000, "2017-06-05 1:2:13"),
    (4, "DDD", "dept1", 1500, "2019-08-10 10:52:53"),
    (5, "EEE", "dept2", 8000, "2016-01-11 5:52:43"),
    (6, "FFF", "dept2", 7200, "2015-04-14 19:32:33"),
    (7, "GGG", "dept3", 7100, "2019-02-21 15:42:43"),
    (8, "HHH", "dept3", 3700, "2016-09-25 15:32:33"),
    (9, "III", "dept3", 4500, "2017-10-15 15:22:23"),
    (10, "JJJ", "dept5", 3400, "2018-12-17 15:14:17")]
empdf = spark.createDataFrame(emp, ["id", "name", "dept", "salary", "date"])
display(empdf)

# COMMAND ----------

from pyspark.sql.functions import * 
empdf.withColumn("new_date",date_format("date","dd MMMM yyyy")).display()

# COMMAND ----------

empdf.withColumn("current",current_timestamp())\
.withColumn("diff",date_diff("current","date")).display()

# COMMAND ----------

empdf.withColumn("current",current_timestamp())\
.withColumn("diff",months_between("current","date",True)).display()

# COMMAND ----------

empdf.printSchema()

# COMMAND ----------

empdf.withColumn("new_date", to_date(empdf["date"])).printSchema()

# COMMAND ----------

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.display()

# COMMAND ----------

df.filter("state='NY'").explain()

# COMMAND ----------

df.groupBy("department").count().explain()

# COMMAND ----------

from pyspark.sql.functions import sum, min, max

df.groupBy("department").agg(
    sum("salary").alias("sumofsalary"),
    min("salary").alias("minofsalary"),
    max("salary").alias("maxofsalary")
).display()

# COMMAND ----------

customer_data = [(1001, 'Alice', 'Johnson', '2024-01-15'),
(1002, 'Bob', 'Smith', '2024-01-18'),
(1003, 'Carol', 'Davis', '2024-01-22'),
(1004, 'David', 'Miller', '2024-01-25'),
(1005, 'Emily', 'Martinez', '2024-01-28'),
(1006, 'Frank', 'Taylor', '2024-01-30'),
(1007, 'Grace', 'Anderson', '2024-02-02'),
(1008, 'Harry', 'White', '2024-02-05'),
(1009, 'Iris', 'Brown', '2024-02-08'),
(1010, 'Jack', 'Wilson', '2024-02-12')]
customer_schema = ['Customer_id','First_name','Last_name','Order_date']
customer_df = spark.createDataFrame(data=customer_data,schema=customer_schema)

# COMMAND ----------

sales_data = [
    (1, 1001, 'ProductA', 2, 50.0, '2024-01-15'),
    (2, 1002, 'ProductB', 1, 75.0, '2024-01-18'),
    (3, 1003, 'ProductC', 3, 30.0, '2024-01-22'),
    (4, 1004, 'ProductA', 1, 50.0, '2024-01-25'),
    (5, 1005, 'ProductB', 2, 75.0, '2024-01-28'),
    (6, 1006, 'ProductC', 1, 30.0, '2024-01-30'),
    (7, 1007, 'ProductA', 2, 50.0, '2024-02-02'),
    (8, 1008, 'ProductB', 1, 75.0, '2024-02-05'),
    (9, 1009, 'ProductC', 3, 30.0, '2024-02-08'),
    (10, 1010, 'ProductA', 1, 50.0, '2024-02-12'),
    # Adding some repeated entries for demonstration
    (11, 1002, 'ProductB', 1, 75.0, '2024-01-18'),  # Customer 1002 with ProductB repeated
    (12, 1006, 'ProductC', 1, 30.0, '2024-01-30')]
sales_schema = ['OrderID','CustomerID','Product','Quantity','Price','OrderDate']
sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)

# COMMAND ----------

sales_schema = ['OrderID','CustomerID','Product','Quantity','Price','OrderDate']

# COMMAND ----------

sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)

# COMMAND ----------

customer_schema = ['Customer_id','First_name','Last_name','Order_date']

# COMMAND ----------

customer_df = spark.createDataFrame(data=customer_data,schema=customer_schema)

# COMMAND ----------

sort_merge_df = customer_df.join(sales_df,customer_df["Customer_id"]==sales_df["CustomerID"], "inner")

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

df=customer_df.join(broadcast(sales_df),customer_df["Customer_id"]==sales_df["CustomerID"], "inner")

# COMMAND ----------

df.display()

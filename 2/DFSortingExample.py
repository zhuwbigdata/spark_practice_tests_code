# Databricks notebook source
simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# sort by column names as string
df.sort("department","state").show(truncate=False)

# COMMAND ----------

# sort by COL
from  pyspark.sql.functions import col
df.sort(col("department"),col("state")).show(truncate=False)

# COMMAND ----------

# orderBy to sort using column name as string
df.orderBy("department","state").show(truncate=False)

# COMMAND ----------

# orderBy to  sort using COL
from  pyspark.sql.functions import col
df.orderBy(col("department"),col("state")).show(truncate=False)

# COMMAND ----------

df.sort(df.department.asc(),df.state.asc()).show(truncate=False)

# COMMAND ----------

# using functions
from pyspark.sql.functions import asc, desc
df.sort(asc("department"), asc("state")).show(truncate=False)

# COMMAND ----------

# using functions
from pyspark.sql.functions import asc, desc, col
df.sort(asc(col("department")), asc(col("state"))).show(truncate=False)

# COMMAND ----------

# COL.asc / desc
df.sort(col("department").asc(),col("state").asc()).show(truncate=False)

# COMMAND ----------


df.orderBy(col("department").asc(),col("state").asc()).show(truncate=False)

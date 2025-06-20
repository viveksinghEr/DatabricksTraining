# Databricks notebook source
spark



# COMMAND ----------

user_data=([(1,'Naval'),(2,'Karan')])
user_schema="id int, name string"

df=spark.createDataFrame(data=user_data, schema=user_schema)

# COMMAND ----------

df.select("*")

# COMMAND ----------

df.select("*").display()


# COMMAND ----------

df.select("id").display()

# COMMAND ----------

from pyspark.sql.functions import *



# COMMAND ----------

df.select(col("id").alias("emp_id")).display()


# COMMAND ----------

df1=df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()
   

# COMMAND ----------

df.withColumnsRenamed({"id:emp_id","name:emp_name"}).display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name"}).display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name"}).display()


# COMMAND ----------

df.withColumnRenamed("id","emp_id").withColumnRenamed("name","emp_name").display()


# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name"}).display()


# COMMAND ----------

df2=df.withColumn("ingestion_date",current_date())

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %fs ls
# MAGIC

# COMMAND ----------

input_path="dbfs:/FileStore/circuits.csv"
#df=spark.read.csv("dbfs:/FileStore/eclerx_input_files/circuits.csv")
#df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/eclerx_input_files/circuits.csv")
df=spark.read.csv(input_path,header=True,inferSchema=True)

# COMMAND ----------

df.display()


# COMMAND ----------

#Task: 
    1. rename circuitId-- circuit_id, circuitRef -- circuit_ref
    2. add new column with current_date
    3. drop url col

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").display()


# COMMAND ----------

df.withColumnRenamed("circuitRef","circuit_ref")

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn("ingestion_date",current_date()

# COMMAND ----------



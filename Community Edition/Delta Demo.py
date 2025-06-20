# Databricks notebook source
print("hello")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table Ctemp (CId int, cname varchar(100))

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into Ctemp values(1,'Vivek'),(2, 'Naval')

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended hive_metastore.default.Ctemp
# MAGIC

# COMMAND ----------



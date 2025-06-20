# Databricks notebook source
print("Hello")

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC Create database VivekDB
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Create schema dbo
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table tbl(TId int, Tname varchar(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into tbl values(1,'Vivek'),
# MAGIC (2,'Vivek'),(3,'Vivek')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC Update  tbl
# MAGIC Set Tname='Aadvika'
# MAGIC Where Tid=2
# MAGIC

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl

-- Databricks notebook source
Create table demo(id int, name string)


-- COMMAND ----------

Insert into demo values(1,'a')

-- COMMAND ----------

Insert into demo values(2,'b');
Insert into demo values(3,'c');
Insert into demo values(4,'d');
Insert into demo values(5,'e');
Insert into demo values(6,'f');
Insert into demo values(7,'g');

-- COMMAND ----------

select * from demo

-- COMMAND ----------

OPTIMIZE demo 
ZORDER BY (id)

-- COMMAND ----------

desc detail demo

-- COMMAND ----------

delete from demo

-- COMMAND ----------

describe history demo


-- COMMAND ----------

restore table demo to version as of 8

-- COMMAND ----------

select * from demo

-- COMMAND ----------

vacuum demo retain 0 hours

-- COMMAND ----------

spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum demo retain 0 hours

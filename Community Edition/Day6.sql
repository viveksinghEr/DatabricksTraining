-- Databricks notebook source
create table emp(id int, name string, city string)

-- COMMAND ----------

--create table emp(id int, name string, city string)
--desc extended emp



-- COMMAND ----------

-- MAGIC %fs head

-- COMMAND ----------

dbfs:/user/hive/warehouse/emp/_delta_log/00000000000000000000.json

desc history emp

insert into emp values(1,'a','Mumbai')

select * from emp

desc detail emp

-- COMMAND ----------

-- MAGIC %fs head
-- MAGIC dbfs:/user/hive/warehouse/emp/_delta_log/00000000000000000000.json
-- MAGIC

-- COMMAND ----------


desc history emp


-- COMMAND ----------


insert into emp values(1,'Vivek','Mumbai')

--select * from emp



-- COMMAND ----------

select * from emp

-- COMMAND ----------

desc detail emp

-- COMMAND ----------

-- MAGIC %fs head
-- MAGIC dbfs:/user/hive/warehouse/emp/_delta_log/00000000000000000000.json

-- COMMAND ----------

desc detail emp

-- COMMAND ----------

desc history emp


-- COMMAND ----------

select * from emp

-- COMMAND ----------

insert into emp values(2,'Singh','Pune'),(3,'Vivek2','Mumbai')

-- COMMAND ----------

select * from emp

-- COMMAND ----------

desc extended emp

-- COMMAND ----------

desc detail emp

-- COMMAND ----------

Insert into emp values(4,'e1','c1');
Insert into emp values(5,'e2','c2');

-- COMMAND ----------



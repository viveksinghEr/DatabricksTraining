-- Databricks notebook source
create table demo_vendors (id int, name string) location '/FileStore/eclerx_metadata/vendors'

-- COMMAND ----------

insert into demo_vendors values(1,'a')  

-- COMMAND ----------

drop table demo

-- COMMAND ----------

drop table demo_vendors

-- COMMAND ----------



-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark

-- COMMAND ----------

-- MAGIC %fs ls
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv", header=True, inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.saveAsTable("bike_day")

-- COMMAND ----------

create or replace view max_month as 
select mnth,round(max(temp),2) as max from bike_day group by mnth order by max desc

-- COMMAND ----------

select * from max_month


-- COMMAND ----------

-- MAGIC %python
-- MAGIC describe history max_month

-- COMMAND ----------


desc extended max_month

-- COMMAND ----------

create or replace temp view holiday_count_temp as 
select mnth, count(*)  as count 
from hive_metastore.default.bike_day 
where holiday =1 group by mnth

-- COMMAND ----------

show views


-- COMMAND ----------

select mnth, count(*)  as count 
from bike_day 
where holiday =1 group by mnth

-- COMMAND ----------

create or replace function fullName(fName string, lName string)
returns string
return concat(fname," ",lname)

-- COMMAND ----------

select fullName("vivek","Singh")as asd

-- COMMAND ----------

create or replace function a(age int)
returns string
return case when age >60 then 'senior' 
when age <60 and age>20  then 'adult'
when age <20 then 'teenage'  
end

-- COMMAND ----------

insert into 

-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Note: Make sure your cluster is using Databricks Runtime 7.2 or above !
-- MAGIC #### If running in Databricks Community - some steps may take a few minutes to run

-- COMMAND ----------

-- MAGIC %run ./Utils

-- COMMAND ----------

select count(*) from customer

-- COMMAND ----------

select count(*) from region

-- COMMAND ----------

select count(*) from nation

-- COMMAND ----------

select count(*) from supplier

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Goal of this Short Demo is to show how Delta can be used as a data store for Datawarehouse Style Data & Reporting and built by SQL Engineers
-- MAGIC <img src='https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png' width=700>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Lets take a look at the Sample 3NF Relational Data Model (Source Data - TPCH Sample Dataset)
-- MAGIC <br>
-- MAGIC <img src='https://kejser.org/wp-content/uploads/2014/06/image_thumb2.png' width=500>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Delta Lake as the Foundation
-- MAGIC <img src='https://i.ibb.co/wc1jf7v/1-img2.png'>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src='https://i.ibb.co/zQHhFcg/modelling.png'>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###For this example we are going to build a Simple Star Schema
-- MAGIC #### Create Some Dimensions

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS dw_demo;

-- COMMAND ----------

select * from nation limit 3

-- COMMAND ----------

select * from region limit 3;

-- COMMAND ----------

select
  row_number() over (
    ORDER BY
      n_nationkey
  ) as NATION_SKEY,
  n_nationkey as NATION_ID,
  initcap(n_name) as NATION_NAME,
  n_comment as NATION_COMMENT,
  r_regionkey as REGION_ID,
  r_name as REGION_NAME,
  r_comment as REGION_COMMENT,
  'Y' as CURRENT_RECORD,
  cast('1900-01-01 00:00:00' as timestamp) as START_DATE,
  cast(null as timestamp) as END_DATE
from
  nation n,
  region r
where
  n_regionkey = r_regionkey

-- COMMAND ----------

/**************************
Nation / Region Dimension  
***************************/
CREATE OR REPLACE TABLE dw_demo.region_dim
USING DELTA
as select row_number() over (ORDER BY n_nationkey) as NATION_SKEY,
n_nationkey as NATION_ID, initcap(n_name) as NATION_NAME,n_comment as NATION_COMMENT,r_regionkey as REGION_ID,r_name as REGION_NAME,r_comment as REGION_COMMENT,
'Y' as CURRENT_RECORD, cast('1900-01-01 00:00:00'as timestamp) as START_DATE, cast(null as timestamp) as END_DATE
from nation n, region r
where n_regionkey = r_regionkey

-- COMMAND ----------

SELECT * FROM dw_demo.region_dim limit 10

-- COMMAND ----------

select * from customer limit 3;

-- COMMAND ----------

/*****************
Customer Dimension  
******************/
CREATE OR REPLACE TABLE dw_demo.customer_dim
USING DELTA
as select row_number() over (ORDER BY c_custkey) as CUSTOMER_SKEY,c_custkey as CUSTOMER_ID, c_name as CUSTOMER_NAME,c_phone as CUSTOMER_PHONE,c_acctbal as CURRENT_CUSTOMER_BAL,C_mktsegment as CUSTOMER_MKTSEGMENT,c_comment as CUSTOMER_COMMENT,'Y' as CURRENT_RECORD,cast('1900-01-01 00:00:00'as timestamp) as START_DATE, cast(null as timestamp) as END_DATE
from customer c

-- COMMAND ----------

SELECT * FROM dw_demo.customer_dim limit 3

-- COMMAND ----------

/*****************
Supplier Dimension  
******************/
CREATE OR REPLACE TABLE dw_demo.supplier_dim
USING DELTA
as
select row_number() over (ORDER BY s_suppkey) as SUPPLIER_SKEY, s_suppkey as SUPPLIER_ID, s_name as SUPPLIER_NAME, s_acctbal as CURRENT_SUPPLIER_BAL, s_nationkey as SUPPLIER_NATION_ID, s_comment as SUPPLIER_COMMENT,'Y' as CURRENT_RECORD,cast('1900-01-01 00:00:00' as timestamp) as START_DATE, cast(null as timestamp) as END_DATE
from supplier s

-- COMMAND ----------

SELECT * FROM dw_demo.supplier_dim limit 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Some Facts

-- COMMAND ----------

/***************
Orders Fact Table
****************/
CREATE OR REPLACE TABLE dw_demo.orders_fact
USING DELTA
/* Join Orders and Customer Data */
with orders as (select
o_orderkey as ORDER_ID, o_custkey as CUSTOMER_ID, c_nationkey as NATION_ID, o_orderstatus as ORDER_STATUS, o_comment as ORDER_COMMENT,
add_months(o_orderdate,259) as ORDER_DATE, o_totalprice as ORDER_TOTALPRICE
from orders o, customer c
where o_custkey = c_custkey
and add_months(o_orderdate,259) < '2020-03-01 00:00:00')

/* Query to Join Tables & get the correct SKEYS */
select c_dim.CUSTOMER_SKEY,r_dim.NATION_SKEY, o.*  from orders o

/* Get the Customer SKEY record */
join dw_demo.customer_dim c_dim
on o.CUSTOMER_ID = c_dim.CUSTOMER_ID
and ORDER_DATE between c_dim.start_date and nvl(c_dim.end_date,'2999-12-31 00:00:00')

/* Get the Region SKEY record */
join dw_demo.region_dim r_dim
on o.NATION_ID = r_dim.NATION_ID
and ORDER_DATE between r_dim.start_date and nvl(r_dim.end_date,'2999-12-31 00:00:00')

-- COMMAND ----------

select * from dw_demo.orders_fact order by CUSTOMER_sKEY;

-- COMMAND ----------

/***************
Orders Item line Fact Table
****************/
CREATE OR REPLACE TABLE dw_demo.orders_item_fact
USING DELTA
/* Join Orders, Orders Line Item and Customer Data */
with orders as (select
o_orderkey as ORDER_ID, o_custkey as CUSTOMER_ID, c_nationkey as NATION_ID, add_months(o_orderdate,259) as ORDER_DATE, o_orderstatus as ORDER_STATUS, o_comment as ORDER_COMMENT,
l_linenumber as LINE_NUMBER, l_quantity as LINE_QUANTITY, l_extendedprice as LINE_PRICE, l_discount as LINE_DISCOUNT,
l_tax as LINE_TAX, l_returnflag as LINE_RETURNFLAG, l_linestatus as LINE_STATUS, add_months(l_shipdate,259) as SHIP_DATE,
add_months(l_receiptdate,259) as RECEIPT_DATE, l_suppkey as SUPPLIER_ID
from orders o, lineitem li,customer c
where o_orderkey = l_orderkey
and o_custkey = c_custkey
and add_months(o_orderdate,259) < '2020-03-01 00:00:00')

/* Query to Join Tables & get the correct SKEYS */
select c_dim.CUSTOMER_SKEY,r_dim.NATION_SKEY,s_dim.SUPPLIER_SKEY, o.*  from orders o

/* Get the Customer SKEY record */
join dw_demo.customer_dim c_dim
on o.CUSTOMER_ID = c_dim.CUSTOMER_ID
and ORDER_DATE between c_dim.start_date and nvl(c_dim.end_date,'2999-12-31 00:00:00')

/* Get the Region SKEY record */
join dw_demo.region_dim r_dim
on o.NATION_ID = r_dim.NATION_ID
and ORDER_DATE between r_dim.start_date and nvl(r_dim.end_date,'2999-12-31 00:00:00')

/* Get the Supplier SKEY record */
join dw_demo.supplier_dim s_dim
on o.SUPPLIER_ID = s_dim.SUPPLIER_ID
and ORDER_DATE between s_dim.start_date and nvl(s_dim.end_date,'2999-12-31 00:00:00')

-- COMMAND ----------

SELECT COUNT(1) FROM dw_demo.orders_item_fact

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### We now have a simple Star Schema Design
-- MAGIC <img src='https://i.ibb.co/86cnHwN/simple-star-schema.png'>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### What Happends if data needs to change? No Problem! We can use Merge

-- COMMAND ----------

select * from  dw_demo.supplier_dim
where supplier_id = 900

-- COMMAND ----------

create or replace temp view supplier_updates as
select 900 as SUPPLIER_ID, 'Supplier#000000900' as SUPPLIER_NAME, 100 as CURRENT_SUPPLIER_BAL, 6 as SUPPLIER_NATION_ID, 'Overwriting Example' as SUPPLIER_COMMENT, current_timestamp as START_DATE

-- COMMAND ----------

select * from supplier_updates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Use Delta's Merge functinality to perform an SCD2 update

-- COMMAND ----------

use dw_demo;
-- ========================================
-- Merge SQL 
-- ========================================
MERGE INTO supplier_dim
USING (
   -- These rows will either UPDATE the details existing suppliers or INSERT the new details of suppliers
  SELECT supplier_updates.supplier_id as mergeKey, cast(null as long) as new_skey, supplier_updates.*
  FROM supplier_updates
  UNION ALL
  -- These rows will INSERT new details of existing suppliers 
  -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
  SELECT NULL as mergeKey, max_skey + row_number() over (order by dim.supplier_id) as new_skey,  updates.*
  FROM supplier_updates updates JOIN supplier_dim dim
  ON updates.supplier_id = dim.supplier_id 
  CROSS JOIN  (select max(supplier_skey) as max_skey from supplier_dim) maxskey
  WHERE dim.current_record = 'Y' AND (updates.SUPPLIER_NAME <> dim.SUPPLIER_NAME 
  OR updates.CURRENT_SUPPLIER_BAL <> dim.CURRENT_SUPPLIER_BAL 
  OR updates.SUPPLIER_NATION_ID <> dim.SUPPLIER_NATION_ID 
  OR updates.SUPPLIER_COMMENT <> dim.SUPPLIER_COMMENT )
  
) staged_updates
ON supplier_dim.supplier_id = mergeKey

WHEN MATCHED AND supplier_dim.current_record = 'Y' AND (supplier_dim.SUPPLIER_NAME <> staged_updates.SUPPLIER_NAME
  OR supplier_dim.CURRENT_SUPPLIER_BAL <> staged_updates.CURRENT_SUPPLIER_BAL OR supplier_dim.SUPPLIER_NATION_ID <> staged_updates.SUPPLIER_NATION_ID
  OR supplier_dim.SUPPLIER_COMMENT <> staged_updates.SUPPLIER_COMMENT)
  THEN UPDATE SET current_record = 'N', end_date = staged_updates.start_date    -- Set current to false and endDate to source's effective date.
  
WHEN NOT MATCHED THEN 
  INSERT(supplier_skey, supplier_id, supplier_name, current_supplier_bal, supplier_nation_id, supplier_comment, current_record, start_date, end_date) 
  VALUES(staged_updates.new_skey,staged_updates.supplier_id, staged_updates.supplier_name,  staged_updates.current_supplier_bal, staged_updates.supplier_nation_id, staged_updates.supplier_comment,
  'Y', staged_updates.start_date, null) -- Set current to true along with the new details and its effective date.

-- COMMAND ----------

select * from  dw_demo.supplier_dim
where supplier_id = 900

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What about streaming?
-- MAGIC #### First lets Clone the fact table!
-- MAGIC ##### Delta has ability to do a Zero Data Copy clone (usefull for being able to test changes to a table without copying entire dataset and also to be able to let people maniupate without affecting Prod)

-- COMMAND ----------

create or replace table dw_demo.orders_fact_stream shallow clone dw_demo.orders_fact

-- COMMAND ----------

select count(1) from dw_demo.orders_fact_stream

-- COMMAND ----------

select * from dw_demo.orders_fact_stream limit 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import time
-- MAGIC i = 1
-- MAGIC while i <= 6:
-- MAGIC   # Execute Insert statement
-- MAGIC   insert_sql = "INSERT INTO dw_demo.orders_fact_stream VALUES (3091, 3, 7744839, 3091, 2, 'O', 'Test Order', current_date(), 50000)"
-- MAGIC   spark.sql(insert_sql)
-- MAGIC   print('streaming_fact_data: inserted new row of data, loop: [%s]' % i)
-- MAGIC     
-- MAGIC   # Loop through
-- MAGIC   i = i + 1
-- MAGIC   time.sleep(3)
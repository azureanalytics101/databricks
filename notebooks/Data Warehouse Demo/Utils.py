# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType,DateType


orders_schema = StructType([
      StructField("o_orderkey", LongType()),
      StructField("o_custkey", LongType()),
      StructField("o_orderstatus", StringType()),
      StructField("o_totalprice", DoubleType()),
      StructField("o_orderdate", DateType()),
      StructField("o_orderpriority", StringType()),
      StructField("o_clerk", StringType()),
      StructField("o_shippriority", IntegerType()),
      StructField("o_comment", StringType())
])

customer_schema = StructType([
    StructField("c_custkey", IntegerType()),
    StructField("c_name", StringType()),
    StructField("c_address", StringType()),
    StructField("c_nationkey", IntegerType()),
    StructField("c_phone", StringType()),
    StructField("c_acctbal", DoubleType()),
    StructField("c_mktsegment", StringType()),
    StructField("c_comment", StringType())
])

part_schema = StructType([
      StructField("p_partkey", LongType()),
      StructField("p_name",StringType()),
      StructField("p_mfgr", StringType()),
      StructField("p_brand", StringType()),
      StructField("p_type", StringType()),
      StructField("p_size", IntegerType()),
      StructField("p_container", StringType()),
      StructField("p_retailprice", DoubleType()),
      StructField("p_comment", StringType())
])
      
lineitem_schema = StructType([
      StructField("l_orderkey", LongType()),
      StructField("l_partkey", LongType()),
      StructField("l_suppkey", LongType()),
      StructField("l_linenumber", IntegerType()),
      StructField("l_quantity", DoubleType()),
      StructField("l_extendedprice", DoubleType()),
      StructField("l_discount", DoubleType()),
      StructField("l_tax", DoubleType()),
      StructField("l_returnflag", StringType()),
      StructField("l_linestatus", StringType()),
      StructField("l_shipdate", DateType()),
      StructField("l_commitdate", DateType()),
      StructField("l_receiptdate", DateType()),
      StructField("l_shipinstruct", StringType()),
      StructField("l_shipmode", StringType()),
      StructField("l_comment", StringType())
])


supplier_schema = StructType([
       StructField("s_suppkey", LongType()),
       StructField("s_name", StringType()),
       StructField("s_address", StringType()),
       StructField("s_nationkey", LongType()),
       StructField("s_phone", StringType()),
       StructField("s_acctbal", DoubleType()),
       StructField("s_comment", StringType())
])
  
partssupp_schema = StructType([
       StructField("ps_partkey", LongType()),
       StructField("ps_suppkey", LongType()),
       StructField("ps_availqty", IntegerType()),
       StructField("ps_supplycos", DoubleType()),
       StructField("ps_comment", StringType())
])
     
    
nation_schema = StructType([
       StructField("n_nationkey", LongType()),
       StructField("n_name", StringType()),
       StructField("n_regionkey", LongType()),
       StructField("n_comment", StringType())
])

region_schema = StructType([
       StructField("r_regionkey", LongType()),
      StructField("r_name", StringType()),
       StructField("r_comment", StringType())
])

# COMMAND ----------

customer_df = \
spark.read.option("delimiter","|")\
.schema(customer_schema)\
.csv("dbfs:/databricks-datasets/tpch/data-001/customer")

customer_df.createOrReplaceTempView("customer")

# COMMAND ----------

part_df = \
spark.read.option("delimiter","|")\
.schema(part_schema)\
.csv("dbfs:/databricks-datasets/tpch/data-001/part")

part_df.createOrReplaceTempView("part")

# COMMAND ----------

lineitem_df = \
spark.read.option("delimiter","|")\
.schema(lineitem_schema)\
.csv("dbfs:/databricks-datasets/tpch/data-001/lineitem")

lineitem_df.createOrReplaceTempView("lineitem")

# COMMAND ----------

orders_df = \
spark.read.option("delimiter","|")\
.schema(orders_schema)\
.csv("dbfs:/databricks-datasets/tpch/data-001/orders")

orders_df.createOrReplaceTempView("orders")

# COMMAND ----------

supplier_df = \
spark.read.option("delimiter","|")\
.schema(supplier_schema)\
.csv("dbfs:/databricks-datasets/tpch/data-001/supplier")\

supplier_df.createOrReplaceTempView("supplier")

# COMMAND ----------

partssupp_df = \
spark.read.option("delimiter","|")\
.schema(partssupp_schema)\
.csv("dbfs:/databricks-datasets/tpch/data-001/partsupp")\

partssupp_df.createOrReplaceTempView("partsupp")

# COMMAND ----------

nation_df = \
spark.read.option("delimiter","|")\
.schema(nation_schema)\
.csv("dbfs:/databricks-datasets/tpch/data-001/nation")\

nation_df.createOrReplaceTempView("nation")

# COMMAND ----------

region_df = \
spark.read.option("delimiter","|")\
.schema(region_schema)\
.csv("dbfs:/databricks-datasets/tpch/data-001/region")\

region_df.createOrReplaceTempView("region")
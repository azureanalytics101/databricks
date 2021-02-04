# Databricks notebook source
# MAGIC %md 
# MAGIC ### Reset Widgets

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Widgets with default values

# COMMAND ----------

dbutils.widgets.text("Persisted_File_Path", "/vendeltalakeadw/dbo/TestDeltaLakeAutomate")
dbutils.widgets.text("Persisted_Table", "dbo_TestDeltaLakeAutomate")
dbutils.widgets.text("Merge_Condition", "sink.SomeId = source.SomeId")
dbutils.widgets.text("Merge_Filter", " AND SomeInt < 10")
dbutils.widgets.text("Primary_Key", "AddressID")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Secrets from KeyVault

# COMMAND ----------

secretScope = "key-vault-secrets"
secretServicePrincipalId = "ServicePrincipalClientId"
secretServicePrincipalKey = "ServicePrincipalKey"
secretTenantId = "TenantId"
adlsFileSystem = "persisted"
adlsAccountName = "vendeltalakeadls"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmount DB if required

# COMMAND ----------

try:
  dbutils.fs.unmount("/mnt/" + adlsFileSystem)
except Exception as e:
  print("{} already unmounted".format(adlsFileSystem))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount ADLS Delta Lake on to databricks

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = secretScope, key = secretServicePrincipalId),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = secretScope, key = secretServicePrincipalKey),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + dbutils.secrets.get(scope = secretScope, key = secretTenantId) + "/oauth2/token"}
# Mount ADLS file system
try:
  dbutils.fs.mount(
    source = "abfss://" + adlsFileSystem + "@" + adlsAccountName +".dfs.core.windows.net/",
    mount_point = "/mnt/" + adlsFileSystem,
    extra_configs = configs)
except Exception as e:
  print("Error: {} already mounted.  Run unmount first".format(adlsFileSystem))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetch Source Data

# COMMAND ----------

jdbcHostname = "vendeltalakedbsvr.database.windows.net"
jdbcUsername = "vendeltalakeadmin"
jdbcDatabase = "vendeltalakeadw"
jdbcPassword = "Sreedh@r1971"
jdbcPort = "1433"
persistedFileSystem = "persisted"
persistedDatabase = "persisted"
transientView = "transientView"
warehouse = "dwh"

persistedFilePath = dbutils.widgets.get("Persisted_File_Path")
persistedTable = dbutils.widgets.get("Persisted_Table")
condition = dbutils.widgets.get("Merge_Condition")
reduce = dbutils.widgets.get("Merge_Filter")
pk = dbutils.widgets.get("Primary_Key");

query = "(select * from SalesLT.{TABLENAME} where {PRIMARYKEY} > -1) as DeltaLakeSource".format(TABLENAME=persistedTable, PRIMARYKEY = pk)
persistedMountPath = "/mnt/" + persistedFileSystem + persistedFilePath

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

dfSource = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties).createOrReplaceTempView(transientView)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS {DATABASE}".format(DATABASE = persistedDatabase));
spark.sql("""CREATE TABLE IF NOT EXISTS {DATABASE}.{TABLE}
             USING DELTA
             LOCATION '{PATH}'
             AS SELECT * FROM {VIEW_TRANS}""".format(DATABASE = persistedDatabase, TABLE = persistedTable, PATH = persistedMountPath, VIEW_TRANS = transientView));
spark.sql("""MERGE INTO {DATABASE}.{TABLE} AS sink
USING {View_Trans} AS source
  ON {CONDITION}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *""".format(DATABASE=persistedDatabase, TABLE=persistedTable,View_Trans=transientView, CONDITION = condition ));
spark.sql("""DELETE FROM {DATABASE}.{TABLE} as sink
             WHERE NOT EXISTS (SELECT 1 
                  FROM {View_Trans} as source
                  where {CONDITION})""".format(DATABASE=persistedDatabase, TABLE=persistedTable,View_Trans=transientView,CONDITION = condition));

# COMMAND ----------

latest_version = spark.sql("SELECT max(version) FROM (DESCRIBE HISTORY persisted.address)").collect()
df = spark.sql('select * from persisted.address version as of {version}'.format(version=latest_version[0][0]))
display(df)

# COMMAND ----------

spark.sql("Create database if not exists {DATAWAREHOUSE}".format(DATAWAREHOUSE=warehouse))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dwh.DIM_Address
# MAGIC USING DELTA
# MAGIC AS
# MAGIC select
# MAGIC   row_number() over (
# MAGIC     order by
# MAGIC       AddressID
# MAGIC   ) as Address_S,
# MAGIC   AddressID,
# MAGIC   AddressLine1,
# MAGIC   AddressLine2,
# MAGIC   City,
# MAGIC   StateProvince as State,
# MAGIC   CountryRegion as Country,
# MAGIC   PostalCode as Zip,
# MAGIC   cast('1900-01-01 00:00:00' as timestamp) as EFFECTIVE_START_DATE,
# MAGIC   cast(null as timestamp) as EFFECTIVE_END_DATE
# MAGIC from
# MAGIC   persisted.address

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW address_updates as
# MAGIC select * from persisted.address version as of 4
# MAGIC except
# MAGIC select * from persisted.address version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dwh.DIM_Address
# MAGIC USING (SELECT AddressID as mergeKey, cast(null as long) as new_skey, address_updates.* FROM address_updates)
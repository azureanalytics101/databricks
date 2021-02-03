# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# create widgets
dbutils.widgets.text("Persisted_File_Path", "/vendeltalakeadw/dbo/TestDeltaLakeAutomate")
dbutils.widgets.text("Persisted_Table", "dbo_TestDeltaLakeAutomate")
dbutils.widgets.text("Merge_Condition", "sink.SomeId = source.SomeId")
dbutils.widgets.text("Merge_Filter", " AND SomeInt < 10")

# COMMAND ----------

secretScope = "key-vault-secrets"
secretServicePrincipalId = "ServicePrincipalClientId"
secretServicePrincipalKey = "ServicePrincipalKey"
secretTenantId = "TenantId"
adlsFileSystem = "persisted"
adlsAccountName = "vendeltalakeadls"

# COMMAND ----------

try:
  dbutils.fs.unmount("/mnt/" + adlsFileSystem)
except Exception as e:
  print("{} already unmounted".format(adlsFileSystem))

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

jdbcHostname = "vendeltalakedbsvr.database.windows.net"
jdbcUsername = "vendeltalakeadmin"
jdbcDatabase = "vendeltalakeadw"
jdbcPassword = "P@ssw0rd987!"
jdbcPort = "1433"
query = "(select * from dbo.TestDeltaLakeAutomate where someid > -1) as DeltaLakeSource"

persistedFileSystem = "persisted"

persistedDatabase = "persisted"
transientView = "transientView"

persistedFilePath = dbutils.widgets.get("Persisted_File_Path")
persistedTable = dbutils.widgets.get("Persisted_Table")
condition = dbutils.widgets.get("Merge_Condition")
reduce = dbutils.widgets.get("Merge_Filter")

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

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS {DATABASE}.{TABLE}
             USING DELTA
             LOCATION '{PATH}'
             AS SELECT * FROM {VIEW_TRANS}""".format(DATABASE = persistedDatabase, TABLE = persistedTable, PATH = persistedMountPath, VIEW_TRANS = transientView))

# COMMAND ----------

spark.sql("""MERGE INTO {DATABASE}.{TABLE} AS sink
USING {View_Trans} AS source
  ON {CONDITION}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *""".format(DATABASE=persistedDatabase, TABLE=persistedTable,View_Trans=transientView, CONDITION = condition ))

# COMMAND ----------

spark.sql("""DELETE FROM {DATABASE}.{TABLE} as sink
             WHERE NOT EXISTS (SELECT 1 
                  FROM {View_Trans} as source
                  where {CONDITION})""".format(DATABASE=persistedDatabase, TABLE=persistedTable,View_Trans=transientView,CONDITION = condition))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM persisted.dbo_testdeltalakeautomate

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from persisted.dbo_testdeltalakeautomate version as of 7

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended persisted.dbo_testdeltalakeautomate

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history persisted.dbo_testdeltalakeautomate

# COMMAND ----------

# MAGIC %sql
# MAGIC analyze table persisted.dbo_testdeltalakeautomate compute statistics noscan;

# COMMAND ----------


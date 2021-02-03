# Databricks notebook source
# Select Libraries => Install New => Select Library Source = "Maven" => Coordinates => Search Packages => Select Maven Central => Search for the package required. Example: mysql-connector-java library => Select the version required => Install

# COMMAND ----------

# dbutils.widgets.removeAll()
# Storage Path in Lake
dbutils.widgets.text("Persisted_File_Path", "/vendeltalakeadw/appian") 

# TableName
dbutils.widgets.text("Persisted_Table", "Permit")
dbutils.widgets.text("Merge_Condition", "sink.permit_id = source.permit_id")
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

jdbcHostname = "prodappianreporting.mysql.database.azure.com"
username = "prodReporting@prodappianreporting"
password = "PXwLgnfbgEAzIJZx"
jdbcDatabase = "appian"
jdbcPort = 3306

# COMMAND ----------

jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "com.mysql.jdbc.Driver"
}

# COMMAND ----------

tablename = dbutils.widgets.get("Persisted_Table")
tableView = "transview"
pushdown_query = "(select * from {0}) {1}".format(tablename, tablename)
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
df.createOrReplaceTempView(tableView)

# COMMAND ----------

database = "persisted"
spark.sql("CREATE DATABASE IF NOT EXISTS {DATABASE}".format(DATABASE = database))
persistedFileSystem = "persisted"
mountPath = "/mnt/" + persistedFileSystem + dbutils.widgets.get("Persisted_File_Path") + "/" + tablename
spark.sql("""CREATE TABLE IF NOT EXISTS {DATABASE}.{TABLE}
             USING DELTA 
             LOCATION '{PATH}'
             AS SELECT * FROM {VIEW_TRANS}""".format(DATABASE = database, TABLE = tablename, PATH = mountPath, VIEW_TRANS = tableView))

condition = dbutils.widgets.get("Merge_Condition")
spark.sql("""MERGE INTO {DATABASE}.{TABLE} AS sink
USING {View_Trans} AS source
  ON {CONDITION}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *""".format(DATABASE=database, TABLE=tablename, View_Trans=tableView, CONDITION = condition ))

spark.sql("""DELETE FROM {DATABASE}.{TABLE} as sink
             WHERE NOT EXISTS (SELECT 1 
                  FROM {View_Trans} as source
                  where {CONDITION})""".format(DATABASE=database, TABLE=tablename,View_Trans=tableView,CONDITION = condition))

# COMMAND ----------

# optimize
display(spark.sql("optimize {db}.{table}".format(db = database, table = tablename)))

# COMMAND ----------

display(spark.sql("describe history {DATABASE}.{TABLE}".format(DATABASE = database, TABLE = tablename)))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from persisted.Permit_Application version as of 1 where permit_application_id = 69201
# MAGIC union
# MAGIC select * from persisted.Permit_Application version as of 2  where permit_application_id = 69201
# MAGIC union
# MAGIC select * from persisted.Permit_Application version as of 3  where permit_application_id = 69201

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from persisted.Permit version as of 3
# MAGIC except
# MAGIC select * from persisted.Permit version as of 1

# COMMAND ----------


# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "60f9a5a8-d447-4d5b-9a9d-9c6a8681ad35",
"fs.azure.account.oauth2.client.secret": "5nC8Q~HEJPLsGEQf7cRggQHl8Xdx1UMZcSN46aH7",
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/498d1fa7-172e-4231-95cc-12b957ca7c62/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@rupamolympicdata.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/tokyoolymic")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic/raw-data"

# COMMAND ----------

athletes = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/teams.csv")

# COMMAND ----------

# teams.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType())) \
                             .withColumn("Male", col("Male").cast(IntegerType())) \
                             .withColumn("Total", col("Total").cast(IntegerType()))

entriesgender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

# find the top countries with the highest number of gold medals
top_gold_medals_countries = medals.orderBy("Gold", ascending=False).select("Team_Country", "Gold")
top_gold_medals_countries.show()

# COMMAND ----------


# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

# write the data on adls gen2

athletes.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/athletes")
coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/teams")

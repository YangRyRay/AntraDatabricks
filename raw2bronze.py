# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Ingest Data

# COMMAND ----------

rootPath = "/movie/"

rawPath = rootPath + "raw/"
bronzePath = rootPath + "bronze/"

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS ryang_movies")
spark.sql("USE ryang_movies")

# COMMAND ----------

dbutils.fs.rm(rootPath,recurse=True)

# COMMAND ----------

storage_account_name = "trainingblob5607"
storage_account_access_key = "weAKyLCfYMuwzWPxRgIQnGamPz6kNjYtBwlTpNJInNcvnhngHhq8mCH45llTxC9FXW+LPVOXT2Gk+AStVOLxnA=="

file_location = "wasbs://movie@trainingblob5607.blob.core.windows.net/"
file_type = "text"

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

from azure.storage.blob import BlockBlobService 
block_blob_service = BlockBlobService(account_name=storage_account_name, account_key=storage_account_access_key) 
files = [] 
generator = block_blob_service.list_blobs('movie')
for file in generator:
    dbutils.fs.cp(file_location+file.name, rawPath+file.name)
    files.append(rawPath+file.name)

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

print(
    dbutils.fs.head(
        dbutils.fs.ls("dbfs:/movie/raw/")[0].path
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Raw to Bronze

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

print(files)

# COMMAND ----------

movie_data_df = (
    spark.read.option("multiline","true").json(files))
display(movie_data_df)

# COMMAND ----------

from pyspark.sql.functions import explode

movie_data_exploded_df=movie_data_df.select(explode(movie_data_df.movie).alias("movie"))

# COMMAND ----------

movie_data_exploded_df = movie_data_exploded_df.dropDuplicates()
print(movie_data_exploded_df.count())
display(movie_data_exploded_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

movie_df = movie_data_exploded_df.select(
    "movie",
    lit("Manual Upload").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate"))

# COMMAND ----------

display(movie_df)

# COMMAND ----------

from pyspark.sql.functions import col
(movie_df.select(
        "datasource",
        "ingesttime",
        "movie",
        "status",
        col("ingestdate").alias("p_ingestdate"),
    )
.write.format("delta")
.mode("append")
.partitionBy("p_ingestdate")
.save(bronzePath))


# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_bronze
"""
)

spark.sql(
    f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

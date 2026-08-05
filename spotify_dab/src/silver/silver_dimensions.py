# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

import os
import sys

project_pth = os.path.join(os.getcwd(), '..', '..')
sys.path.append(project_pth)
from utils.transformations import reusable

# COMMAND ----------

dbutils.fs.rm("abfss://def-strm-check@azureproject2106.dfs.core.windows.net/_checkpoints/", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimUser

# COMMAND ----------

# MAGIC %md
# MAGIC ### AutoLoader

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@azureproject2106.dfs.core.windows.net/DimUser/checkpoint")\
    .load("abfss://bronze@azureproject2106.dfs.core.windows.net/DimUser")
    

# COMMAND ----------

display(df_user)

# COMMAND ----------

df_user = df_user.withColumn("user_name",upper(col("user_name")))
display(df_user)

# COMMAND ----------

df_user_obj = reusable()
df_user = df_user_obj.dropColumn(df_user,['_rescued_data'])
df_user = df_user.dropDuplicates(['user_id'])
display(df_user)

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@azureproject2106.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@azureproject2106.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimArtist

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@azureproject2106.dfs.core.windows.net/DimArtist/checkpoint")\
    .load("abfss://bronze@azureproject2106.dfs.core.windows.net/DimArtist")

# COMMAND ----------

display(df_art)

# COMMAND ----------

df_art_obj = reusable()
df_art = df_art_obj.dropColumn(df_art,['_rescued_data'])
df_art = df_art.dropDuplicates(['artist_id'])
display(df_art)

# COMMAND ----------

df_art.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@azureproject2106.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@azureproject2106.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_cata.silver.DimArt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimTrack

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@azureproject2106.dfs.core.windows.net/DimTrack/checkpoint")\
    .load("abfss://bronze@azureproject2106.dfs.core.windows.net/DimTrack")

# COMMAND ----------

display(df_track)

# COMMAND ----------

df_track = df_track.withColumn("durationFlag", when(col("duration_sec")<150, "Low")\
                                            .when(col("duration_sec")>300, "High")\
                                            .otherwise("Medium"))
df_track = df_track.withColumn("track_name", regexp_replace(col("track_name"), '-', ' '))
df_track = reusable().dropColumn(df_track,['_rescued_data'])
display(df_track)

# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@azureproject2106.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@azureproject2106.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimDate

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@azureproject2106.dfs.core.windows.net/DimDate/checkpoint")\
    .load("abfss://bronze@azureproject2106.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date = reusable().dropColumn(df_date,['_rescued_data'])

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@azureproject2106.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@azureproject2106.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FactStream

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@azureproject2106.dfs.core.windows.net/FactStream/checkpoint")\
    .load("abfss://bronze@azureproject2106.dfs.core.windows.net/FactStream")

# COMMAND ----------

display(df_fact)

# COMMAND ----------

df_fact = reusable().dropColumn(df_fact,['_rescued_data'])

df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@azureproject2106.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@azureproject2106.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_cata.silver.FactStream")
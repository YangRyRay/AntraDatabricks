# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Run Raw2Bronze

# COMMAND ----------

# %run ./raw2bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Start Bronze2Silver

# COMMAND ----------

bronzeDF = spark.read.table("movie_bronze").filter("status = 'new'")


# COMMAND ----------

display(bronzeDF)

# COMMAND ----------

# Not used

from pyspark.sql.types import *

schema = StructType([
    StructField("backdropUrl",StringType(),True),
    StructField("Budget",DoubleType(),True),
    StructField("CreatedBy",StringType(),True),
    StructField("CreatedDate",TimestampType(),True),
    StructField("Id",LongType(),True),
    StructField("ImdbUrl",StringType(),True),
    StructField("OriginalLanguage",StringType(),True),
    StructField("Overview",StringType(),True),
    StructField("PosterUrl",StringType(),True),
    StructField("Price",DoubleType(),True),
    StructField("ReleaseDate",TimestampType(),True),
    StructField("Revenue",DoubleType(),True),
    StructField("Runtime",LongType(),True),
    StructField("Tagline",StringType(),True),
    StructField("Title",StringType(),True),
    StructField("TmdbUrl",StringType(),True),
    StructField("UpdatedBy",StringType(),True),
    StructField("UpdatedDate",TimestampType(),True),
    StructField("genres",StructType([
        StructField("Id",IntegerType(),True),
        StructField("Name",StringType(),True),
    ]))
    
])

# bronzeDF_aug = bronzeDF.withColumn("nested_json", from_json(col("movie"), schema))

# COMMAND ----------

display(bronzeDF)

# COMMAND ----------

silverDF = bronzeDF.select("movie","movie.*")
silverDF = silverDF.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Quarantine Bad Data

# COMMAND ----------

silverDF_quarantine = silverDF.where(silverDF.RunTime<0)
silverDF_clean = silverDF.where(silverDF.RunTime>=0)

display(silverDF_quarantine)

# COMMAND ----------

from delta.tables import DeltaTable
bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverQuarantine = silverDF_quarantine.withColumn(
    "status", lit("quarantined")
)

(
    bronzeTable.alias("bronze")
    .merge(silverQuarantine.alias("quarantine"), "bronze.movie = quarantine.movie")
    .whenMatchedUpdate(set={"status": "quarantine.status"})
    .execute()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# Troubleshooting merge

#silverDF.groupBy("Id").count().filter("count > 1").show()
#display(silverDF.where("Id==8747"))
#silverDF.select("Id"==8747)
#checkDupes = bronzeTable.groupBy("movie.Id").count().filter("count > 1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Create Silver Tables

# COMMAND ----------

from pyspark.sql.functions import *

langSilver = silverDF_clean.select("OriginalLanguage").distinct()
langSilver =langSilver.withColumn("id", monotonically_increasing_id())
display(langSilver)

# COMMAND ----------

silverPath = rootPath + "silver/"
silverLangPath =  silverPath + "lang/"

(
    langSilver.select(
        "id","OriginalLanguage"
    )
    .write.format("delta")
    .mode("append")
    .save(silverLangPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS language_silver
"""
)

spark.sql(
    f"""
CREATE TABLE language_silver
USING DELTA
LOCATION "{silverLangPath}"
"""
)

# COMMAND ----------

genreSilver=silverDF_clean.select(explode(silverDF_clean.genres).alias("genres"))
genreSilver=genreSilver.select("genres.id","genres.name").distinct()
genreSilver=genreSilver.where(genreSilver.name!="")
display(genreSilver.sort("id"))

# COMMAND ----------

silverGenrePath =  silverPath + "genre/"
(
    genreSilver.select(
        "id","name"
    )
    .write.format("delta")
    .mode("append")
    .save(silverGenrePath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS Genre_silver
"""
)

spark.sql(
    f"""
CREATE TABLE Genre_silver
USING DELTA
LOCATION "{silverGenrePath}"
"""
)

# COMMAND ----------

movieSilver=silverDF_clean.select("BackdropUrl","Budget","CreatedBy","CreatedDate","Id","ImdbUrl","OriginalLanguage","Overview","PosterUrl","Price","ReleaseDate","Revenue","RunTime","Tagline","Title","TmdbUrl","UpdatedBy","UpdatedDate")

# COMMAND ----------

silverMoviePath =  silverPath + "movie/"
( movieSilver.select("BackdropUrl","Budget","CreatedBy","CreatedDate","Id","ImdbUrl","OriginalLanguage","Overview","PosterUrl","Price","ReleaseDate","Revenue","RunTime","Tagline","Title","TmdbUrl","UpdatedBy","UpdatedDate"
    )
    .write.format("delta")
    .mode("append")
    .save(silverMoviePath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS Movie_silver
"""
)

spark.sql(
    f"""
CREATE TABLE Movie_silver
USING DELTA
LOCATION "{silverMoviePath}"
"""
)

# COMMAND ----------

genreMovieJunctionSilver=silverDF_clean.select("Id",explode(silverDF_clean.genres).alias("genres"))
genreMovieJunctionSilver=genreMovieJunctionSilver.select(col("Id").alias("MovieId"),col("genres.id").alias("GenreId"))
display(genreMovieJunctionSilver)

# COMMAND ----------

silverMovGenPath =  silverPath + "movie_genre_junction/"
(
    genreMovieJunctionSilver.select(
        "MovieId","GenreId"
    )
    .write.format("delta")
    .mode("append")
    .save(silverMovGenPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS Movie_Genre_silver
"""
)

spark.sql(
    f"""
CREATE TABLE Movie_Genre_silver
USING DELTA
LOCATION "{silverMovGenPath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_genre_silver

# COMMAND ----------

from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silverDF_clean.withColumn("status", lit("loaded"))

update_match = "bronze.movie = clean.movie"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Fix quarantined

# COMMAND ----------

qDF = spark.read.table("movie_bronze").filter("status = 'quarantined'")
display(qDF)

# COMMAND ----------

silverqDF = qDF.select("movie","movie.*")

movieSilver=silverqDF.select("BackdropUrl","Budget","CreatedBy","CreatedDate","Id","ImdbUrl","OriginalLanguage","Overview","PosterUrl","Price","ReleaseDate","Revenue",abs("RunTime").alias("RunTime"),"Tagline","Title","TmdbUrl","UpdatedBy","UpdatedDate")

display(movieSilver)

# COMMAND ----------

( movieSilver.select("BackdropUrl","Budget","CreatedBy","CreatedDate","Id","ImdbUrl","OriginalLanguage","Overview","PosterUrl","Price","ReleaseDate","Revenue","RunTime","Tagline","Title","TmdbUrl","UpdatedBy","UpdatedDate"
    )
    .write.format("delta")
    .mode("append")
    .save(silverMoviePath)
)

# COMMAND ----------

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silverqDF.withColumn("status", lit("loaded"))

update_match = "bronze.movie = clean.movie"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Questions
# MAGIC 
# MAGIC Certain movies have valid ID for the genre, but the name of the genre is missing. This does not need to be fixed for this schema, as the genre information for each movie is stored in a junction table anyways. As long as each genre ID has a name associated, this will be fine.
# MAGIC 
# MAGIC Fixing a movie's budget should be fixed in the silver table, data should not be modified in the raw or bronze tables. THis can be acheived by setting up another quarantine, or adding to the other quarantine results where the budget is insufficient.
# MAGIC 
# MAGIC There are a couple things needed to ensure that only one record shows up per movie. At a batch level, we can ensure each batch only has no duplicate records. This can be done by using dropDuplicates() when initiating the silver table, which is what I did. We also need to ensure that data loaded onto the silver table won't be loaded on with subsequent batches. This can be done by checking the status for each record in the bronze table. 

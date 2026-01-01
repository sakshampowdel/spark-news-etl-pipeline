from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import StopWordsRemover

def create_spark_session():
  """
  Initializes a Spark session configured for local execution within a Docker container.

  Returns:
    SparkSession: The authenticated and configured Spark entry point.
  """
  return (SparkSession.builder
          .appName("NewsTrendAnalysis")
          .config("spark.driver.host", "127.0.0.1")
          .config("spark.driver.bindAddress", "127.0.0.1")
          .config("spark.sql.parquet.compression.codec", "snappy")
          .master("local[*]")
          .getOrCreate())

def generate_source_stats(df: DataFrame):
  """
  Calculates daily article volume per source with partitioning.

  Args:
    df (DataFrame): The Silver layer Spark DataFrame containing cleaned news data.

  Returns:
    DataFrame: Aggregated statistics showing article counts by date and source.
  """
  return (df
          .withColumn("date", F.to_date("ingestion_timestamp"))
          .groupBy("date", "source")
          .count()
          .orderBy("date", F.desc("count")))

def generate_top_keywords(df: DataFrame, limit=20):
  """
  Cleans punctuation and filters words using Spark's built-in StopWordsRemover.

  Args:
    df (DataFrame): The Silver layer Spark DataFrame containing cleaned news data.
    limit (int): The maximum number of top keywords to return. Defaults to 20.

  Returns:
    DataFrame: A collection of the most frequent words found in article titles.
  """
  # 1. Split by non-word characters (removes punctuation like commas/dots)
  words_df = df.withColumn("raw_words", F.split(F.lower(F.col("title")), r"\W+"))
  
  # 2. Use Spark's built-in StopWordsRemover for a comprehensive list
  remover = StopWordsRemover(inputCol="raw_words", outputCol="filtered_words")
  # You can add your own custom words to the default English list
  custom_stop_words = remover.getStopWords() + ["u", "s", "says", "new"]
  remover.setStopWords(custom_stop_words)
  
  filtered_df = remover.transform(words_df)
  
  return (filtered_df
          .withColumn("word", F.explode(F.col("filtered_words")))
          .filter(F.length(F.col("word")) > 2) # Filter out "s", "a", "rt"
          .groupBy("word")
          .count()
          .orderBy(F.desc("count"))
          .limit(limit))
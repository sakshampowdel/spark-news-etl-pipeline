from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.feature import Tokenizer, StopWordsRemover

SILVER_SCHEMA = StructType([
  StructField("article_url", StringType(), True),
  StructField("title", StringType(), True),
  StructField("teaser", StringType(), True),
  StructField("source", StringType(), True),
  StructField("ingestion_timestamp", StringType(), True)
])

def create_spark_session():
  """
  Initializes a Spark session configured for local execution within a Docker container.

  Returns:
    SparkSession: The authenticated and configured Spark entry point.
  """
  return (
    SparkSession.builder
    .appName("NewsTrendAnalysis")
    .config("spark.log.level", "ERROR") 
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.ui.showConsoleProgress", "false")
    .master("local[*]")
    .getOrCreate()
  )

def generate_top_keywords(df: DataFrame, limit: int = 20) -> DataFrame:
  """
  Analyzes article titles to identify top trending keywords across news sources.

  Performs text normalization, tokenization, stop-word removal, and pivots 
  the results to show keyword frequency per publisher.

  Args:
    df (DataFrame): The Silver layer Spark DataFrame containing article metadata.
    limit (int): The number of top keywords to return. Defaults to 20.

  Returns:
    DataFrame: A wide-format DataFrame with keywords and counts per source.
  """
  cleaned_df = df.withColumn(
    "cleaned_title", 
    F.regexp_replace(F.lower(F.col("title")), r"[^\w\s]", " ")
  )

  tokenizer = Tokenizer(inputCol="cleaned_title", outputCol="raw_words")
  words_df = tokenizer.transform(cleaned_df)

  remover = StopWordsRemover(inputCol="raw_words", outputCol="filtered_words")

  extra_junk = ["says", "new", "delivered", "morning", "reuters", "npr", "washington"]
  remover.setStopWords(remover.getStopWords() + extra_junk)
  
  filtered_df = remover.transform(words_df)

  exploded_df = (
    filtered_df
    .withColumn("word", F.explode(F.col("filtered_words")))
    .filter(F.length(F.col("word")) > 2)
  )
  

  return (
    exploded_df
    .groupBy("word")
    .pivot("source")
    .count()
    .na.fill(0)
    .withColumn("total", F.expr("NPR + Reuters + `The Washington Post`"))
    .orderBy(F.desc("total"))
    .limit(limit)
  )
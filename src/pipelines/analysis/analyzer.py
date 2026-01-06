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
  """The transformation logic: Words -> Counts."""
  # Clean punctuation and lowercase
  cleaned_df = df.withColumn(
    "cleaned_title", 
    F.regexp_replace(F.lower(F.col("title")), r"[^\w\s]", " ")
  )

  # Convert strzing to list of words
  tokenizer = Tokenizer(inputCol="cleaned_title", outputCol="raw_words")
  words_df = tokenizer.transform(cleaned_df)

  # Filter out common 'stop words'
  remover = StopWordsRemover(inputCol="raw_words", outputCol="filtered_words")
  # Add news-specific junk words to the default list
  extra_junk = ["says", "new", "delivered", "morning", "reuters", "npr", "washington"]
  remover.setStopWords(remover.getStopWords() + extra_junk)
  
  filtered_df = remover.transform(words_df)

  # Explode list into rows and count
  return (
    filtered_df
    .withColumn("word", F.explode(F.col("filtered_words")))
    .filter(F.length(F.col("word")) > 2)
    .groupBy("word")
    .count()
    .orderBy(F.desc("count"))
    .limit(limit)
  )
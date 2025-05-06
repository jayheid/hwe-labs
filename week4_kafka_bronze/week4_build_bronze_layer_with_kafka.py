import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, split

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

def getScramAuthString(username, password):
  return f"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username="{username}"
   password="{password}";
  """

# Define the Kafka broker and topic to read from
kafka_bootstrap_servers = os.environ.get("HWE_BOOTSTRAP")
username = os.environ.get("HWE_USERNAME")
password = os.environ.get("HWE_PASSWORD")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
kafka_topic = "reviews"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week4Lab") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

# Read data from Kafka using the DataFrame API
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
    .load()
#    .select(split(col("value").cast("string"), "\t"))

df.printSchema()
df.createOrReplaceTempView("reviews")
# df = spark.sql("select split(cast(value as string), '\t') AS tsv FROM reviews")
df = spark.sql("SELECT CAST(key AS string) AS key, " \
"CAST(value AS string) AS value, " \
"topic, " \
"partition, " \
"offset, " \
"timestamp, " \
"timestampType " \
"FROM reviews")

df = spark.sql("SELECT " \
"CAST(split(value, '\t')[0] AS string) AS marketplace, " \
"CAST(split(value, '\t')[1] AS string) AS customer_id, " \
"CAST(split(value, '\t')[2] AS string) AS review_id, " \
"CAST(split(value, '\t')[3] AS string) AS product_id, " \
"CAST(split(value, '\t')[4]  AS string) AS product_parent, " \
"CAST(split(value, '\t')[5] AS string) AS product_title, " \
"CAST(split(value, '\t')[6] AS string) AS product_category, " \
"CAST(split(value, '\t')[7] AS int) AS star_rating, " \
"CAST(split(value, '\t')[8] AS int) AS helpful_votes, " \
"CAST(split(value, '\t')[9] AS int) AS total_votes, " \
"CAST(split(value, '\t')[10] AS string) AS vine, " \
"CAST(split(value, '\t')[11] AS string) AS verified_purchase, " \
"CAST(split(value, '\t')[12] AS string) AS review_headline, " \
"CAST(split(value, '\t')[13] AS string) AS review_body, " \
"CAST(split(value, '\t')[14] AS string) AS purchase_date, " \
"current_timestamp() AS review_timestamp " \
"FROM reviews")

df.printSchema()

# Process the received data
query = df \
  .writeStream \
  .outputMode("append") \
  .format("parquet") \
  .option("path", "s3a://hwe-spring-2025/jheidbrink/bronze/reviews") \
  .option("checkpointLocation", "/tmp/kafka-checkpoint") \
  .start()

"""
# write to console
query = df \
  .writeStream \
  .format("console") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kafka-checkpoint") \
  .start()
"""

# Wait for the streaming query to finish
query.awaitTermination()

# Stop the SparkSession
spark.stop()
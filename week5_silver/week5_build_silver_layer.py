import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week5Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .master('local[*]') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

# 1. Define a bronze_schema which describes the Parquet files under the bronze reviews directory on S3
bronze_schema = StructType([
    StructField("marketplace", StringType(), nullable=False)
    ,StructField("customer_id", StringType(), nullable=False)
    ,StructField("review_id", StringType(), nullable=False)
    ,StructField("product_id", StringType(), nullable=False)
    ,StructField("product_parent", StringType(), nullable=False)
    ,StructField("product_title", StringType(), nullable=False)
    ,StructField("product_category", StringType(), nullable=False)
    ,StructField("star_rating", IntegerType(), nullable=False)
    ,StructField("helpful_votes", IntegerType(), nullable=False)
    ,StructField("total_votes", IntegerType(), nullable=False)
    ,StructField("vine", StringType(), nullable=False)
    ,StructField("verified_purchase", StringType(), nullable=False)
    ,StructField("review_headline", StringType(), nullable=False)
    ,StructField("review_body", StringType(), nullable=False)
    ,StructField("purchase_date", StringType(), nullable=False)
    ,StructField("review_timestamp", TimestampType(), nullable=False)])


# Define a streaming dataframe using readStream on top of the bronze reviews directory on S3
bronze_reviews = spark.readStream.schema(bronze_schema).parquet("s3a://hwe-spring-2025/jheidbrink/bronze/reviews")

# Register a virtual view on top of that dataframe
bronze_reviews.createOrReplaceTempView("bronze_reviews")

# Define a non-streaming dataframe using read on top of the bronze customers directory on S3
bronze_customers = spark.read.parquet("s3a://hwe-spring-2025/jheidbrink/bronze/customers")

# Register a virtual view on top of that dataframe
bronze_customers.createOrReplaceTempView("bronze_customers")

# bronze_customers.printSchema()

# Define a silver_data dataframe by
# joining the review and customer data on their common key of customer_id
# applying a business validation rule to prevent unverified reviews in the bronze layer 
#   from being loaded into the silver layer
silver_data = spark.sql("" \
"SELECT " \
"   br.marketplace," \
"   br.customer_id," \
"   bc.customer_name," \
"   bc.gender," \
"   bc.date_of_birth," \
"   bc.city," \
"   bc.state," \
"   br.review_id," \
"   br.product_id," \
"   br.product_parent," \
"   br.product_title," \
"   br.product_category," \
"   br.star_rating," \
"   br.helpful_votes," \
"   br.total_votes," \
"   br.vine," \
"   br.verified_purchase," \
"   br.review_headline," \
"   br.review_body," \
"   br.purchase_date," \
"   br.review_timestamp" \
"   FROM " \
"   bronze_reviews br " \
"   INNER JOIN bronze_customers bc ON br.customer_id = bc.customer_id " \
"WHERE " \
"   br.verified_purchase = 'Y'")

# silver_data.printSchema()

"""
Write that silver data to S3 under s3a://hwe-$CLASS/$HANDLE/silver/reviews using append mode, 
a checkpoint location of /tmp/silver-checkpoint, and a format of parquet
"""

streaming_query = silver_data \
  .writeStream \
  .outputMode("append") \
  .format("parquet") \
  .option("path", "s3a://hwe-spring-2025/jheidbrink/silver/reviews") \
  .option("checkpointLocation", "/tmp/silver-checkpoint")

streaming_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()

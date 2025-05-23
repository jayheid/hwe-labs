import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col
from pyspark.sql.functions import current_timestamp

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week3Lab") \
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

### Questions

# Remember, this week we are using the Spark DataFrame API (and last week was the Spark SQL API).

#Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe.
#You will use the "reviews" dataframe defined here to answer all the questions below...

reviews = spark.read.csv("resources/reviews.tsv.gz",
                         sep = "\t",
                         header = True)

#Question 2: Display the schema of the dataframe.

reviews.printSchema()

#Question 3: How many records are in the dataframe? 
#Store this number in a variable named "reviews_count".
reviewsCount = reviews.count()
print(reviewsCount)

#Question 4: Print the first 5 rows of the dataframe. 
#Some of the columns are long - print the entire record, regardless of length.
reviewFirstFive = reviews.limit(5)
reviewFirstFive.show(truncate=False)

#Question 5: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
#Look at the first 50 rows of that dataframe. 
#Which value appears to be the most common?
productCategory = reviews.select('product_category').limit(50)
productCategory.show()
# Digital Video Games appears to be the most common product category from the selected set

#Question 6: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
#What is the product title for that review? How many helpful votes did it have?
reviews = reviews.withColumn('helpful_votes', col('helpful_votes').cast('int'))
# mostHelpfulReview = reviews.groupby('review_id','product_title').sum('helpful_votes').orderBy('sum(helpful_votes)',ascending=False).limit(1)
# mostHelpfulReview.show(truncate=False)
# A review for SimCity - Limited Edition has 5068 helpful votes

mostHelpfulReview = reviews.sort(desc("helpful_votes")).select("product_title","helpful_votes")
mostHelpfulReview.show(n=1, truncate = False)

#Question 7: How many reviews have a 5 star rating?
reviews = reviews.withColumn('star_rating', col('star_rating').cast('int'))
fiveStarReviews = reviews.filter("star_rating == 5")
print(fiveStarReviews.count())
# 80,677 reviews have a 5 star rating

#Question 8: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
#Create a new dataframe with just those 3 columns, except cast them as "int"s.
#Look at 10 rows from this dataframe.
castIntSet = reviews.selectExpr("CAST(star_rating AS int) AS star_rating",
                                "CAST(helpful_votes AS int) AS helpful_votes",
                                "CAST(total_votes AS int) AS total_votes")
castIntSet
castIntSet.show(n = 10, truncate=False)

#Question 8: Find the date with the most purchases.
#Print the date and total count of the date with the most purchases
mostRecentPurchase = reviews.groupby('purchase_date').count().withColumnRenamed('count','total_purchases').orderBy('total_purchases',ascending=False).limit(1)
mostRecentPurchase.show()

#Question 9: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
#Hint: Check the documentation for a function that can help: https://spark.apache.org/docs/3.1.3/api/python/reference/pyspark.sql.html#functions
#Print the schema and inspect a few rows of data to make sure the data is correctly populated.
reviews = reviews.withColumn('review_timestamp', current_timestamp())
reviews.printSchema()
reviews.select('review_timestamp').show(n=10)

#Question 10: Write the dataframe with load timestamp to s3a://hwe-$CLASS/$HANDLE/bronze/reviews_static in Parquet format.
#Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...
reviews.write.parquet("s3a://hwe-spring-2025/jheidbrink/bronze/reviews_static", mode='overwrite')

#Question 11: Read the tab separated file named "resources/customers.tsv.gz" into a dataframe
#Write to S3 under s3a://hwe-$CLASS/$HANDLE/bronze/customers
#Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...
#There are no questions to answer about this data set right now, but you will use it in a later lab...
customers = spark.read.csv("resources/customers.tsv.gz", sep = "\t", header = True)
customers.write.parquet("s3a://hwe-spring-2025/jheidbrink/bronze/customers", mode = "overwrite")

#Question 12: Display the schema of the customers dataframe.
#This will be useful when you use it in a later lab...
customers.printSchema()

# Stop the SparkSession
spark.stop()

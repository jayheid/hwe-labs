from pyspark.sql import SparkSession

### Setup: Create a SparkSession
spark = SparkSession.builder \
    .appName("lab") \
    .master("local[1]") \
    .getOrCreate()

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

### Questions

# Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".
reviews = spark.read.csv("resources/reviews.tsv.gz", 
                            sep = "\t", 
                            header = True)

# Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.
reviews.createOrReplaceTempView("reviewSet")

# Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
reviews = spark.sql("SELECT *, current_timestamp() FROM reviewSet")

# Question 4: How many records are in the reviews dataframe? 
totalReviews = spark.sql("SELECT COUNT(*) FROM reviewSet")
totalReviews.show()
# There are 145,431 total records in dataframe

# Question 5: Print the first 5 rows of the dataframe. 
# Some of the columns are long - print the entire record, regardless of length.
reviewSubset = spark.sql("SELECT * FROM reviewSet")
reviewSubset.show(n=5, truncate = True)

# Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# Look at the first 50 rows of that dataframe. 
# Which value appears to be the most common?
reviewCategory = spark.sql("SELECT product_category FROM reviewSet LIMIT 50")
reviewCategory.show(truncate = False)
# Digital_Video_Games appears to be the most common value in result

# Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# What is the product title for that review? How many helpful votes did it have?
helpfulReviews = spark.sql("SELECT review_id, product_title, SUM(CAST(helpful_votes AS int)) AS total_helpful_votes " \
    "FROM reviewSet " \
    "GROUP BY 1,2 " \
    "ORDER BY 3 DESC " \
    "LIMIT 1")
# helpfulReviews.show(n=10, truncate=False)
# A review for SimCity - Limited Edition has the most helpful votes of 5068

# Question 8: How many reviews exist in the dataframe with a 5 star rating?
fiveStarRatingReviews = spark.sql("SELECT COUNT(*) FROM reviewSet WHERE star_rating = '5'")
fiveStarRatingReviews.show()
# There are 80,677 records with a 5 star rating

# Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# Create a new dataframe with just those 3 columns, except cast them as "int"s.
# Look at 10 rows from this dataframe.
# star_rating, helpful_votes, total_votes
castIntSet = spark.sql("SELECT CAST(star_rating AS int)," \
    "CAST(helpful_votes AS int)," \
    "CAST(total_votes AS int) " \
    "FROM reviewSet")

# castIntSet.show(n=10)

# Question 10: Find the date with the most purchases.
# Print the date and total count of the date which had the most purchases.
dateOfMostPurchases = spark.sql("SELECT purchase_date, COUNT(*) as total_purchases FROM reviewSet GROUP BY purchase_date ORDER BY total_purchases DESC LIMIT 1")
dateOfMostPurchases.show()


##Question 11: Write the dataframe from Question 3 to your drive in JSON format.
##Feel free to pick any directory on your computer.
##Use overwrite mode.
reviews.write.json("week2_sql/reviews.json", mode='overwrite')

### Teardown
# Stop the SparkSession
spark.stop()

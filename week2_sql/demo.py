from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Demo") \
    .master("local[1]") \
    .getOrCreate()

version = spark.version
print(f"I've started a Spark cluster running {version}")

# import file
scores_data = spark.read.csv("resources/video_game_scores.tsv", 
                             sep = ",", 
                             header = True)

# print schema and 4 record sample of file
scores_data.printSchema()
scores_data.show(n=4, truncate=False)

# export to json
scores_data.write.json("resources/video_game_scores.json")

spark.stop()
    
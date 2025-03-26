from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, desc

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# TODO: Split the Hashtags column into individual hashtags and count the frequency of each hashtag and sort descending
hashtag_counts = posts_df.select(explode(split(col("Hashtags"), ",")).alias("Hashtag")) \
                         .groupBy("Hashtag") \
                         .count() \
                         .orderBy(col("count").desc())

# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)

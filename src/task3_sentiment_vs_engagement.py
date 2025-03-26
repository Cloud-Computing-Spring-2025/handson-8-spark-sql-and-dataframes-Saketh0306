from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col

spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# TODO: Implement the task here
# Positive (> 0.3), Neutral (-0.3 to 0.3), Negative (< -0.3)
posts_df = posts_df.withColumn("Sentiment", 
    when(col("SentimentScore") > 0.3, "Positive")
    .when(col("SentimentScore") < -0.3, "Negative")
    .otherwise("Neutral")
)

# Calculate average likes and retweets per sentiment category
sentiment_stats = posts_df.groupBy("Sentiment").agg(
    avg("Likes").alias("Avg Likes"), 
    avg("Retweets").alias("Avg Retweets")
).orderBy(col("Avg Likes").desc())

# Save result
sentiment_stats.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)

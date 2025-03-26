from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, when

spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# TODO: Implement the task here

# Join posts with users on UserID
joined_df = posts_df.join(users_df, on="UserID")

# Calculate average likes and retweets per AgeGroup
engagement_df = joined_df.groupBy("AgeGroup").agg(
    avg("Likes").alias("Avg Likes"), 
    avg("Retweets").alias("Avg Retweets")
).orderBy(col("Avg Likes").desc())
# Save result
engagement_df.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)

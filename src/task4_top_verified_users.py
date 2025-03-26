from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# TODO: Implement the task here
verified_users = users_df.filter(col("Verified") == True)

# Join datasets on UserID
verified_posts = verified_users.join(posts_df, "UserID")

# Calculate total reach (Likes + Retweets) for each verified user
top_verified = verified_posts.groupBy("Username").agg(
    _sum(col("Likes") + col("Retweets")).alias("Total Reach")
).orderBy(col("Total Reach").desc()).limit(5)
# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)

# Spark SQL and DataFrames: Social Media Sentiment & Engagement Analysis

## Overview
This project involves analyzing a fictional social media dataset using Spark SQL and DataFrames. The goal is to extract insights on hashtag trends, audience engagement, sentiment-driven behavior, and influencer reach. The dataset consists of user posts and account details, enabling in-depth analysis of user behavior and content performance.

## Dataset
The analysis is based on two CSV files:

1. **posts.csv** - Contains information about social media posts.
    - `PostID` (Integer): Unique ID for the post
    - `UserID` (Integer): ID of the user who posted
    - `Content` (String): Text content of the post
    - `Timestamp` (String): Date and time the post was made
    - `Likes` (Integer): Number of likes on the post
    - `Retweets` (Integer): Number of shares/retweets
    - `Hashtags` (String): Comma-separated hashtags used in the post
    - `SentimentScore` (Float): Sentiment score (-1 to 1, where -1 is most negative)

2. **users.csv** - Contains information about user accounts.
    - `UserID` (Integer): Unique user ID
    - `Username` (String): User's handle
    - `AgeGroup` (String): Age category (Teen, Adult, Senior)
    - `Country` (String): Country of residence
    - `Verified` (Boolean): Whether the account is verified

## Assignment Tasks

### 1. Hashtag Trends
**Objective:** Identify trending hashtags by analyzing their frequency across all posts.

**Steps:**
- Extract hashtags from the `Hashtags` column.
- Count occurrences of each hashtag.
- Return the top 10 most frequently used hashtags.

**Expected Output:**
```
Hashtag    Count
#tech      120
#mood      98
#design    85
```

---
### 2. Engagement by Age Group
**Objective:** Analyze user engagement by comparing likes and retweets across different age groups.

**Steps:**
- Join `posts.csv` and `users.csv` on `UserID`.
- Group by `AgeGroup` and compute average likes and retweets.
- Rank age groups based on overall engagement.

**Expected Output:**
```
Age Group   Avg Likes   Avg Retweets
Adult       67.3        25.2
Teen        22.0        5.6
Senior      9.2         1.3
```

---
### 3. Sentiment vs Engagement
**Objective:** Explore the relationship between sentiment and user engagement.

**Steps:**
- Categorize sentiment based on `SentimentScore`:
  - Positive (> 0)
  - Neutral (= 0)
  - Negative (< 0)
- Compute average likes and retweets per sentiment category.

**Expected Output:**
```
Sentiment   Avg Likes   Avg Retweets
Positive    85.6        32.3
Neutral     27.1        10.4
Negative    13.6        4.7
```

---
### 4. Top Verified Users by Reach
**Objective:** Identify the most influential verified users based on total reach.

**Steps:**
- Filter users where `Verified = True`.
- Compute `Total Reach` as `Likes + Retweets` for each user.
- Return the top 5 verified users with the highest total reach.

**Expected Output:**
```
Username      Total Reach
@techie42     1650
@designer_dan 1320
```

## Technologies Used
- Apache Spark (PySpark)
- Spark SQL
- DataFrames API
- CSV Data Processing

## How to Run the Code
1. Load the CSV files into Spark DataFrames.
2. Execute the SQL queries and transformations for each task.
3. Display the results in a tabular format.
4. Interpret the insights derived from the analysis.

## Conclusion
This assignment helps in understanding large-scale structured data processing using Spark SQL. The insights derived from sentiment analysis, engagement trends, and influencer reach are valuable for social media analytics and marketing strategies.


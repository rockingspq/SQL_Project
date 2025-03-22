Yelp Business Data Analysis

Overview

This project analyzes Yelp business and review data to extract valuable insights into business categories, user reviews, and review trends. The queries leverage SQL to process and analyze data stored in relational tables.

Dataset

The project uses two main tables:

tbl_yelp_business: Contains information about businesses, including business_id, name, city, and categories.

tbl_yelp_reviews: Stores reviews for businesses, including review_id, user_id, business_id, review_stars, review_date, and sentiment.

Data Pipeline: Pulling Data from S3 to Snowflake

Step 1: Load Yelp Reviews Data

CREATE OR REPLACE TABLE yelp_reviews (review_text VARIANT);

COPY INTO yelp_reviews
FROM 's3://mybucket-practice-sp/Yelp/'
CREDENTIALS = (
AWS_KEY_ID = '<your_aws_key_id>',
AWS_SECRET_KEY = '<your_aws_secret_key>'
)
FILE_FORMAT = (TYPE = JSON);

SELECT COUNT(\*) FROM yelp_reviews;

Step 2: Load Yelp Business Data

CREATE OR REPLACE TABLE yelp_business (business_text VARIANT);

COPY INTO yelp_business
FROM 's3://mybucket-practice-sp/Yelp/yelp_academic_dataset_business.json'
CREDENTIALS = (
AWS_KEY_ID = '<your_aws_key_id>',
AWS_SECRET_KEY = '<your_aws_secret_key>'
)
FILE_FORMAT = (TYPE = JSON);

SELECT \* FROM yelp_business LIMIT 10;

Step 3: Transform and Create Structured Tables

CREATE OR REPLACE TABLE tbl_yelp_reviews AS
SELECT
review_text:business_id::STRING AS business_id,
review_text:date::DATE AS review_date,
review_text:stars::NUMBER AS review_stars,
review_text:text::STRING AS review_text,
review_text:user_id::STRING AS user_id,
analyze_sentiment(review_text) AS sentiment
FROM yelp_reviews;

CREATE OR REPLACE TABLE tbl_yelp_business AS
SELECT
business_text:business_id::STRING AS business_id,
business_text:name::STRING AS name,
business_text:categories::STRING AS categories,
business_text:city::STRING AS city,
business_text:state::STRING AS state,
business_text:review_count::NUMBER AS review_count,
business_text:stars::NUMBER AS stars
FROM yelp_business;

SELECT \* FROM tbl_yelp_business LIMIT 1000;

Queries and Insights

1. Number of Businesses in Each Category

Counts the number of businesses under each category and sorts them in descending order.

2. Top 10 Users with the Most Reviews for Restaurants

Finds the top 10 users who have reviewed the highest number of unique businesses in the Restaurants category.

3. Most Popular Business Categories Based on Reviews

Determines the most frequently reviewed business categories.

4. Top 3 Most Recent Reviews for Each Business

Retrieves the three most recent reviews for every business.

5. Month with the Highest Number of Reviews

Identifies the month in which the highest number of reviews were posted.

6. Percentage of 5-Star Reviews for Each Business

Calculates the percentage of 5-star reviews for each business.

7. Top 5 Most Reviewed Businesses in Each City

Lists the top 5 businesses in each city based on the number of reviews.

8. Average Review Rating for Businesses with at Least 100 Reviews

Finds businesses with at least 100 reviews and computes their average review rating.

9. Top 10 Users Who Have Written the Most Reviews

Lists the top 10 users who have written the most reviews and the businesses they reviewed.

10. Top 10 Businesses with the Most Positive Reviews

Finds the top 10 businesses with the highest number of reviews marked as positive.

How to Use

Ensure access to a Snowflake instance and an S3 bucket containing the Yelp dataset.

Execute the SQL scripts provided to load, transform, and analyze the data.

Modify queries as needed to extract additional insights.

Requirements

Snowflake account

AWS S3 bucket access

Basic understanding of SQL for query execution

Future Enhancements

Implement visualizations to present the insights graphically.

Optimize queries for performance improvements.

Incorporate more detailed sentiment analysis techniques.

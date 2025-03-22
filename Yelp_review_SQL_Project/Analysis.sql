-- Load Yelp Reviews Data from S3 to Snowflake
CREATE OR REPLACE TABLE yelp_reviews (review_text VARIANT);

COPY INTO yelp_reviews
FROM 's3://mybucket-practice-sp/Yelp/'
CREDENTIALS = (
  AWS_KEY_ID = '<your_aws_key_id>',
  AWS_SECRET_KEY = '<your_aws_secret_key>'
)
FILE_FORMAT = (TYPE = JSON);

SELECT COUNT(*) FROM yelp_reviews;

-- Load Yelp Business Data from S3 to Snowflake
CREATE OR REPLACE TABLE yelp_business (business_text VARIANT);

COPY INTO yelp_business
FROM 's3://mybucket-practice-sp/Yelp/yelp_academic_dataset_business.json'
CREDENTIALS = (
  AWS_KEY_ID = '<your_aws_key_id>',
  AWS_SECRET_KEY = '<your_aws_secret_key>'
)
FILE_FORMAT = (TYPE = JSON);

SELECT * FROM yelp_business LIMIT 10;

-- Transform and Create Structured Tables
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

SELECT * FROM tbl_yelp_business LIMIT 1000;

-- Query: Number of Businesses in Each Category
WITH cte AS (
  SELECT business_id, TRIM(a.value) AS category
  FROM tbl_yelp_business,
  LATERAL SPLIT_TO_TABLE(categories, ',') a
)
SELECT category, COUNT(*) AS no_of_business
FROM cte
GROUP BY 1
ORDER BY 2 DESC;

-- Query: Top 10 Users Who Reviewed the Most Restaurants
SELECT r.user_id, COUNT(DISTINCT b.business_id)
FROM tbl_yelp_business b
INNER JOIN tbl_yelp_reviews r ON r.business_id = b.business_id
WHERE b.categories ILIKE '%restaurant%'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- Query: Most Popular Business Categories Based on Reviews
WITH cte AS (
  SELECT business_id, TRIM(a.value) AS category
  FROM tbl_yelp_business,
  LATERAL SPLIT_TO_TABLE(categories, ',') a
)
SELECT category, COUNT(*) AS review_count
FROM cte
JOIN tbl_yelp_reviews r ON cte.business_id = r.business_id
GROUP BY 1
ORDER BY 2 DESC;

-- Query: Top 3 Most Recent Reviews for Each Business
WITH cte AS (
  SELECT r.*, b.name,
         ROW_NUMBER() OVER (PARTITION BY r.business_id ORDER BY review_date DESC) AS rn
  FROM tbl_yelp_reviews r
  JOIN tbl_yelp_business b ON b.business_id = r.business_id
)
SELECT * FROM cte WHERE rn <= 3;

-- Query: Month with the Highest Number of Reviews
SELECT MONTH(review_date) AS review_month, COUNT(*) AS no_of_reviews
FROM tbl_yelp_reviews
GROUP BY 1
ORDER BY 2 DESC;

-- Query: Percentage of 5-Star Reviews for Each Business
SELECT b.business_id, b.name, COUNT(*) AS total_reviews,
       SUM(CASE WHEN r.review_stars = 5 THEN 1 ELSE 0 END) AS star_5_reviews,
       (SUM(CASE WHEN r.review_stars = 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percent_review
FROM tbl_yelp_reviews r
JOIN tbl_yelp_business b ON r.business_id = b.business_id
GROUP BY 1, 2;

-- Query: Top 5 Most Reviewed Businesses in Each City
WITH cte AS (
  SELECT b.city, b.business_id, b.name, COUNT(*) AS total_reviews
  FROM tbl_yelp_reviews r
  JOIN tbl_yelp_business b ON r.business_id = b.business_id
  GROUP BY 1, 2, 3
)
SELECT * FROM cte
QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY total_reviews DESC) <= 5;

-- Query: Average Review Rating for Businesses with at Least 100 Reviews
SELECT b.business_id, b.name, COUNT(*) AS total_reviews,
       AVG(review_stars) AS avg_rating
FROM tbl_yelp_reviews r
JOIN tbl_yelp_business b ON r.business_id = b.business_id
GROUP BY 1, 2
HAVING COUNT(*) >= 100
ORDER BY 4 DESC;

-- Query: Top 10 Users with the Most Reviews and Their Businesses
WITH cte AS (
  SELECT user_id, COUNT(*) AS total_reviews
  FROM tbl_yelp_reviews
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 10
)
SELECT user_id, business_id
FROM tbl_yelp_reviews
WHERE user_id IN (SELECT user_id FROM cte);

-- Query: Top 10 Businesses with the Highest Positive Reviews
SELECT r.business_id, b.name, COUNT(*) AS total_positive_reviews
FROM tbl_yelp_reviews r
JOIN tbl_yelp_business b ON r.business_id = b.business_id
WHERE r.sentiment = 'Positive'
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10;

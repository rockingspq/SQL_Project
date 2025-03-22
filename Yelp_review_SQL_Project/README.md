# Yelp Business Data Analysis

## Overview

This project analyzes **Yelp business** and **review data** to extract valuable insights into business categories, user reviews, and review trends. The data is processed and analyzed using SQL queries on relational tables.

---

## Dataset

The project uses two main tables:

- **tbl_yelp_business**: Contains business-related information, including `business_id`, `name`, `city`, and `categories`.
- **tbl_yelp_reviews**: Stores review data for businesses, including `review_id`, `user_id`, `business_id`, `review_stars`, `review_date`, and `sentiment`.

---

## Data Pipeline: Pulling Data from S3 to Snowflake

### Step 1: Load Yelp Reviews Data

```sql
CREATE OR REPLACE TABLE yelp_reviews (review_text VARIANT);

COPY INTO yelp_reviews
FROM 's3://mybucket-practice-sp/Yelp/'
CREDENTIALS = (
    AWS_KEY_ID = '<your_aws_key_id>',
    AWS_SECRET_KEY = '<your_aws_secret_key>'
)
FILE_FORMAT = (TYPE = JSON);

SELECT COUNT(*) FROM yelp_reviews;

CREATE OR REPLACE TABLE yelp_business (business_text VARIANT);

COPY INTO yelp_business
FROM 's3://mybucket-practice-sp/Yelp/yelp_academic_dataset_business.json'
CREDENTIALS = (
    AWS_KEY_ID = '<your_aws_key_id>',
    AWS_SECRET_KEY = '<your_aws_secret_key>'
)
FILE_FORMAT = (TYPE = JSON);

SELECT * FROM yelp_business LIMIT 10;

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

---

## How to Use

1. Ensure access to a **Snowflake** instance and an **S3 bucket** containing the Yelp dataset.
2. Execute the provided SQL scripts to **load**, **transform**, and **analyze** the data.
3. Modify the queries as needed to extract additional insights.

---

## Requirements

- Snowflake account
- AWS S3 bucket access
- Basic understanding of SQL for query execution

---

## Future Enhancements

- **Visualizations**: Implement graphical representations of the insights.
- **Query Optimization**: Improve query performance for large datasets.
- **Sentiment Analysis**: Enhance sentiment analysis with more advanced techniques.

---

Feel free to explore and modify the code to meet your specific needs. Happy analyzing! 🚀


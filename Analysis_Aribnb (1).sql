-- Databricks notebook source
CREATE TEMPORARY VIEW airbnb
USING JDBC
OPTIONS (
  url "jdbc:sqlserver://azuresqlserver2000.database.windows.net:1433;database=azuresqlDB2000;",
  dbtable "AirbnbTomaterialized",
  user 'anandasql',
  password 'ananda@09'
)

-- COMMAND ----------

select* from airbnb

-- COMMAND ----------

select count(*) as TotalRecords from airbnb

-- COMMAND ----------

select count(distinct listing_id) from airbnb

-- COMMAND ----------

select count(distinct city) from airbnb

-- COMMAND ----------

WITH RankedCities AS (
  SELECT  city, 
         COUNT(instant_bookable) AS bookings,
         ROW_NUMBER() OVER (ORDER BY COUNT(instant_bookable) DESC) AS rn
  FROM airbnb
  GROUP BY city
)
SELECT city, bookings
FROM RankedCities
WHERE rn > 2
ORDER BY bookings DESC
limit 5

-- COMMAND ----------

select city,count(instant_bookable) as Total_bookings from airbnb
group by city 
order by Total_bookings desc

-- COMMAND ----------

select city,avg(price) as better_value from airbnb 
group by city order by better_value 


-- COMMAND ----------

select host_id,count(listing_id) from airbnb group by host_id order by count(listing_id)

-- COMMAND ----------

select country,count(distinct host_id) as No_of_host from airbnb where country!='unknown' group by country 
order by No_of_host desc limit 5

-- COMMAND ----------

select count(distinct host_id) as total_host from airbnb

-- COMMAND ----------

select count(distinct country) as countries from airbnb

-- COMMAND ----------

-- Explode amenities and calculate average monetary value
CREATE OR REPLACE TEMP VIEW exploded_hosts AS
SELECT
    host_id,
    price,
    amenity
FROM
    airbnb
LATERAL VIEW explode(split(amenities, ',')) AS amenity;

-- Aggregate average money value per amenity
SELECT
    amenity,
    AVG(price) AS average_money_value
FROM
    exploded_hosts
GROUP BY
    amenity
ORDER BY
    average_money_value DESC
    limit 5

-- COMMAND ----------

SELECT
    host_since_date,
    COUNT(review_id) AS no_of_reviews
FROM airbnb
GROUP BY host_since_date
limit 20


-- COMMAND ----------

SELECT property_type, AVG(price) AS avg_price
FROM airbnb
GROUP BY property_type
ORDER BY avg_price DESC


-- COMMAND ----------

SELECT property_type, COUNT(listing_id) AS total_listings
FROM airbnb
GROUP BY property_type
ORDER BY total_listings DESC


-- COMMAND ----------

SELECT
    month_reviewed,
    COUNT(review_id) AS no_of_reviews
FROM airbnb
GROUP BY month_reviewed
ORDER BY no_of_reviews DESC


-- COMMAND ----------

select count(distinct reviewer_id) as nof_of_reviewers from airbnb 

-- COMMAND ----------

 select host_id,name,count(listing_id) as count from airbnb group by host_id,name order by  count desc limit 10

-- COMMAND ----------

SELECT host_is_superhost, AVG(price) AS avg_price
FROM airbnb
GROUP BY host_is_superhost


-- COMMAND ----------

SELECT date_reviewed , COUNT(review_id) AS review_count
FROM airbnb
GROUP BY date_reviewed
ORDER BY date_reviewed 


-- COMMAND ----------

select country,count(distinct host_id) as No_of_host from airbnb where country!='unknown' group by country 
order by No_of_host desc limit 5

-- COMMAND ----------

SELECT neighbourhood, AVG(price) AS avg_price
FROM airbnb
GROUP BY neighbourhood
order by avg_price desc
limit 5


-- COMMAND ----------

-- Calculate the number of days each host has been active
WITH host_tenure AS (
    SELECT
        listing_id,
        host_id,
        host_since_date,
        DATEDIFF('2024-08-01', host_since_date) AS days_active, -- Directly use the date here
        price,
        review_scores_rating,
        host_acceptance_rate
    FROM airbnb
),

-- Booking frequency by host tenure
booking_frequency AS (
    SELECT
        host_id,
        days_active,
        COUNT(listing_id) AS booking_count
    FROM host_tenure
    GROUP BY host_id, days_active
),

-- Average price by host tenure
avg_price_by_tenure AS (
    SELECT
        days_active,
        AVG(price) AS avg_price
    FROM host_tenure
    GROUP BY days_active
),

-- Average review scores by host tenure
avg_review_scores AS (
    SELECT
        days_active,
        AVG(review_scores_rating) AS avg_rating
    FROM host_tenure
    GROUP BY days_active
),

-- Average host acceptance and response rates by host tenure
avg_acceptance_response_rates AS (
    SELECT
        days_active,
        AVG(host_acceptance_rate) AS avg_acceptance_rate
    FROM host_tenure
    GROUP BY days_active
)

-- Combine all the results into a single view
SELECT
    t.days_active,
    t.avg_price,
    r.avg_rating,
    a.avg_acceptance_rate,
    COALESCE(b.booking_count, 0) AS booking_count
FROM avg_price_by_tenure t
LEFT JOIN avg_review_scores r ON t.days_active = r.days_active
LEFT JOIN avg_acceptance_response_rates a ON t.days_active = a.days_active
LEFT JOIN (
    SELECT
        days_active,
        SUM(booking_count) AS booking_count
    FROM booking_frequency
    GROUP BY days_active
) b ON t.days_active = b.days_active
where t.days_active>0
ORDER BY t.days_active;

-- COMMAND ----------



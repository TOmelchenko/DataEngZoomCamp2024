# Week 3 Homework
### Important Note:

For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York City Taxi Data found here:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.
Stop with loading the files into a bucket.

NOTE: You will need to use the PARQUET option files when creating an External Table

**SETUP**:
Create an external table using the Green Taxi Trip Records Data for 2022.
```
CREATE OR REPLACE EXTERNAL TABLE `dezc2024-taxi-data.ny_taxi_data.external_green_tripdata_2022`
OPTIONS (
  format = 'parquet',
  uris = ['https://storage.cloud.google.com/mage_dezoomcamp2024_1/green_taxi_data_2022.parquet']
);
```
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).

```-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dezc2024-taxi-data.ny_taxi_data.green_tripdata_2022_non_partitoned AS
SELECT * FROM dezc2024-taxi-data.ny_taxi_data.external_green_tripdata_2022;
```

# Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??

- 65,623,481
- 840,402 - this is the answer
- 1,936,423
- 253,647

# Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table - this is the answer
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table 
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/bigquery_1.png)
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/bigquery_2.png)


# Question 3:
How many records have a fare_amount of 0?

- 12,488
- 128,219
- 112
- 1,622 - this is the answer

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/bigquery_3.png)


# Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime Cluster on PUlocationID - this is the answer
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

```
CREATE OR REPLACE TABLE dezc2024-taxi-data.ny_taxi_data.green_tripdata_2022_partitoned_clustered
PARTITION BY DATE(lpep_pickup_date)
CLUSTER BY pulocationid AS
SELECT * FROM dezc2024-taxi-data.ny_taxi_data.external_green_tripdata_2022;
```

# Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table - this is the answer
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/bigquery_4.png)
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/bigquery_5.png)

# Question 6:
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket - this is the answer
- Big Table
- Container Registry

# Question 7:
It is best practice in Big Query to always cluster your data:

- True
- False - this is the answer

# (Bonus: Not worth points) Question 8:
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

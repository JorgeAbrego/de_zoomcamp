-- Creating an external table referring to GCS path
CREATE OR REPLACE EXTERNAL TABLE `vld-dtc-de-00001.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_vld-dtc-de-00001/green_taxi_data/green_tripdata_2022-*.parquet']
);

-- Creating a non partitioned table from external table (aka BigQuery table)
CREATE OR REPLACE TABLE vld-dtc-de-00001.ny_taxi.green_tripdata_non_partitoned AS
SELECT * 
FROM vld-dtc-de-00001.ny_taxi.external_green_tripdata;

/*-- Question 1: --*/
-- What is count of records for the 2022 Green Taxi Data?

SELECT COUNT(1) 
FROM vld-dtc-de-00001.ny_taxi.green_tripdata_non_partitoned;
-- Answer: 840,402 records

/*-- Question 2: --*/
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

-- External Table:
SELECT COUNT(DISTINCT PULocationID) 
FROM vld-dtc-de-00001.ny_taxi.external_green_tripdata; 
-- Answer: 258 records | 0B estimated

-- Table
SELECT COUNT(DISTINCT PULocationID) 
FROM vld-dtc-de-00001.ny_taxi.green_tripdata_non_partitoned; 
-- Answer: 258 records | 6.41MB estimated

/*-- Question 3: --*/
-- How many records have a fare_amount of 0?

SELECT COUNT(1) 
FROM vld-dtc-de-00001.ny_taxi.green_tripdata_non_partitoned
WHERE fare_amount = 0; 
-- Answer: 1,622 records

/*-- Question 4: --*/
-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID 
-- and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

-- Answer: Partition by lpep_pickup_datetime Cluster on PUlocationID

-- Partition by lpep_pickup_datetime Cluster on PUlocationID
CREATE OR REPLACE TABLE vld-dtc-de-00001.ny_taxi.green_tripdata_partitoned
PARTITION BY DATE(lpep_pickup_datetime) AS
SELECT * FROM vld-dtc-de-00001.ny_taxi.external_green_tripdata;

-- Create a partitioned and clustered table from external table
CREATE OR REPLACE TABLE vld-dtc-de-00001.ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM vld-dtc-de-00001.ny_taxi.external_green_tripdata;

/*-- Question 5: --*/
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. 
-- Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. 
-- What are these values?

-- Query non partioned table
SELECT COUNT(DISTINCT PULocationID) 
FROM vld-dtc-de-00001.ny_taxi.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
-- Answer: Estimated 12.82MB

-- Query partioned table
SELECT COUNT(DISTINCT PULocationID) 
FROM vld-dtc-de-00001.ny_taxi.green_tripdata_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
-- Answer: estimated 1.12MB

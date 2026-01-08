CREATE SCHEMA IF NOT EXISTS TAXI.RAW;

CREATE OR REPLACE FILE FORMAT parquet_taxi
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY';
    

CREATE OR REPLACE FILE FORMAT csv_taxi
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
DATE_FORMAT = 'DD-MM-YYYY'
TIME_FORMAT = 'HH24:MI:SS';


CREATE OR REPLACE TABLE TAXI.RAW.RAW_TAXI_TRIPS (
    VendorID INTEGER,
    tpep_pickup_date DATE,
    tpep_pickup_time TIME,
    tpep_dropoff_date DATE,
    tpep_dropoff_time TIME,
    passenger_count INTEGER,
    trip_distance FLOAT,
    RatecodeID INTEGER,
    store_and_fwd_flag STRING,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT,
    cbd_congestion_fee FLOAT
);



CREATE OR REPLACE STORAGE INTEGRATION taxi_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::375039967232:role/snowflake-s3'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://taxi-bucket-parquet/'
  );

DESC STORAGE INTEGRATION taxi_s3_integration;

SHOW STORAGE INTEGRATIONS;

CREATE OR REPLACE STAGE TAXI.RAW.TAXI_S3_STAGE
  URL = 's3://taxi-bucket-parquet/comp_taxi_data.csv'
  STORAGE_INTEGRATION = taxi_s3_integration
  FILE_FORMAT = TAXI.RAW.csv_taxi;

LIST @TAXI.RAW.TAXI_S3_STAGE;


CREATE OR REPLACE PIPE TAXI_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO TAXI.RAW.RAW_TAXI_TRIPS
FROM @TAXI.RAW.TAXI_S3_STAGE
FILE_FORMAT = csv_taxi
ON_ERROR = CONTINUE;

DESC PIPE TAXI_PIPE;

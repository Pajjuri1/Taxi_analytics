{{
  config(
    materialized='view',
    schema='bronze',
    database='dev_taxi',
    tags=['view','bronze']
  )
}}

-- This view directly references the bronze database
-- No transformations, just a clean interface to raw data


SELECT 
  VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  store_and_fwd_flag,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  Airport_fee,
  -- Add metadata
  CURRENT_TIMESTAMP as _source_viewed_at
FROM {{ source('raw', 'yellow_tripdata') }}

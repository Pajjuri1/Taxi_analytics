{{
  config(
    materialized='table',
    schema='silver',
    tags=['silver', 'cleaned'],
    unique_key='trip_id'
  )
}}

WITH raw_trips AS (
  SELECT * FROM {{ ref('src_yellow_trip') }}
),

cleaned_trips AS (
  SELECT 
    -- Generate unique deduplication key
    MD5(
      COALESCE(CAST(VendorID AS VARCHAR), '') || 
      COALESCE(CAST(tpep_pickup_datetime AS VARCHAR), '') || 
      COALESCE(CAST(PULocationID AS VARCHAR), '') || 
      COALESCE(CAST(DOLocationID AS VARCHAR), '') ||
      COALESCE(CAST(fare_amount AS VARCHAR), '')
    ) as trip_id,
    
    -- Parse and standardize timestamps
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    DATE(tpep_pickup_datetime) as pickup_date,
    EXTRACT(HOUR FROM tpep_pickup_datetime) as pickup_hour,
    EXTRACT(MINUTE FROM tpep_pickup_datetime) as pickup_minute,
    
    -- Calculate trip duration in minutes
    DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_minutes,
    
    -- Trip details
    passenger_count,
    trip_distance,
    PULocationID,
    DOLocationID,
    
    -- Payment information
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    
    -- Calculated fields
    CASE 
      WHEN trip_distance > 0 THEN ROUND(fare_amount / trip_distance, 2)
      ELSE NULL 
    END as fare_per_mile,
    
    CASE 
      WHEN fare_amount > 0 THEN ROUND(tip_amount / fare_amount * 100, 2)
      ELSE NULL 
    END as tip_percentage
    
  FROM raw_trips
  -- Apply cleaning rules
  WHERE fare_amount > 0                    -- Rule 1: Positive fare
    AND trip_distance >= 0                 -- Rule 2: Non-negative distance
    AND tpep_dropoff_datetime > tpep_pickup_datetime  -- Rule 3: Valid time order
),

validated_trips AS (
  SELECT 
    c.*,
    -- Validate against zone lookup
    z_pu.Borough as pickup_borough,
    z_pu.Zone as pickup_zone,
    z_do.Borough as dropoff_borough,
    z_do.Zone as dropoff_zone,
    CASE 
      WHEN z_pu.LocationID IS NULL THEN 'invalid_pickup'
      WHEN z_do.LocationID IS NULL THEN 'invalid_dropoff'
      ELSE 'valid' 
    END as location_validation_status
    
  FROM cleaned_trips c
  LEFT JOIN {{ ref('src_taxi_zones') }} z_pu 
    ON c.PULocationID = z_pu.LocationID
  LEFT JOIN {{ ref('src_taxi_zones') }} z_do 
    ON c.DOLocationID = z_do.LocationID
),

deduplicated_trips AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY trip_id 
      ORDER BY tpep_pickup_datetime DESC
    ) as duplicate_rank
  FROM validated_trips
)

SELECT 
  trip_id,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  pickup_date,
  pickup_hour,
  pickup_minute,
  trip_duration_minutes,
  passenger_count,
  trip_distance,
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
  airport_fee,
  fare_per_mile,
  tip_percentage,
  pickup_borough,
  pickup_zone,
  dropoff_borough,
  dropoff_zone,
  location_validation_status,
  -- Pipeline metadata
  CURRENT_TIMESTAMP as silver_processed_at
  
FROM deduplicated_trips
WHERE trip_duration_minutes BETWEEN 0 AND 300  -- Valid duration
  AND location_validation_status = 'valid'     -- Keep only valid locations
  AND duplicate_rank = 1                       -- Remove duplicates





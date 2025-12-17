{{
  config(
    materialized='view',
    schema='bronze',
    database='dev_taxi',
    tags=['source', 'bronze', 'dimension']
  )
}}

-- Source view for taxi zones from bronze database


SELECT 
  LocationID,
  Borough,
  Zone,
  service_zone,
  -- Add metadata
  CURRENT_TIMESTAMP as _source_viewed_at
FROM {{ source('raw', 'taxi_zone_lookup') }}


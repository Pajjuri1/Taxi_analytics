{{
  config(
    materialized='table',
    schema='silver',
    tags=['silver','cleaned']
  )
}}

-- Clean and standardize the taxi zone lookup table
-- This serves as a dimension table for location references

SELECT 
  -- Primary key
  LocationID,
  
  -- Standardized borough names
  CASE 
    WHEN UPPER(Borough) = 'EWR' THEN 'Newark Airport'
    WHEN UPPER(Borough) IN ('MANHATTAN', 'MN') THEN 'Manhattan'
    WHEN UPPER(Borough) = 'BRONX' THEN 'Bronx'
    WHEN UPPER(Borough) = 'BROOKLYN' THEN 'Brooklyn'
    WHEN UPPER(Borough) = 'QUEENS' THEN 'Queens'
    WHEN UPPER(Borough) = 'STATEN ISLAND' THEN 'Staten Island'
    ELSE CONCAT(
  UPPER(SUBSTRING(Borough, 1, 1)),  -- First letter uppercase
  LOWER(SUBSTRING(Borough, 2))      -- Rest lowercase
)
  END as borough_name,
  
  -- Clean zone names
CASE 
  WHEN Zone LIKE '%-%' THEN 
    CONCAT(
      UPPER(SUBSTRING(LOWER(SUBSTRING(Zone, 1, INSTR(Zone, '-') - 1)), 1, 1)),
      SUBSTRING(LOWER(SUBSTRING(Zone, 1, INSTR(Zone, '-') - 1)), 2)
    ) || 
    '-' || 
    CONCAT(
      UPPER(SUBSTRING(LOWER(SUBSTRING(Zone, INSTR(Zone, '-') + 1)), 1, 1)),
      SUBSTRING(LOWER(SUBSTRING(Zone, INSTR(Zone, '-') + 1)), 2)
    )
  ELSE CONCAT(
    UPPER(SUBSTRING(Zone, 1, 1)),
    LOWER(SUBSTRING(Zone, 2))
  )
END as zone_name,
  
  -- Service zone (standardized)
  CASE 
    WHEN UPPER(service_zone) = 'EWR' THEN 'Airport'
    WHEN UPPER(service_zone) = 'BORO ZONE' THEN 'Borough Zone'
    WHEN UPPER(service_zone) = 'YELLOW ZONE' THEN 'Yellow Zone'
        ELSE CONCAT(
  UPPER(SUBSTRING(service_zone, 1, 1)),  -- First letter uppercase
  LOWER(SUBSTRING(service_zone, 2))      -- Rest lowercase
)
  END as service_zone_name,
  
  -- Original values (for reference)
  Borough as original_borough,
  Zone as original_zone,
  service_zone as original_service_zone,
  
  -- Geospatial metadata (if available later)
  NULL as latitude,
  NULL as longitude,
  
  -- Pipeline metadata
  CURRENT_TIMESTAMP as silver_processed_at
  
FROM {{ ref('src_taxi_zones') }}

-- Add any additional cleaning rules
WHERE LocationID IS NOT NULL
  AND Borough IS NOT NULL
  AND Zone IS NOT NULL
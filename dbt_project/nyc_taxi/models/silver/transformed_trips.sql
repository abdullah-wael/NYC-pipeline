select
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
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
    cbd_congestion_fee,
    recalculated_total_amount as total_amount_money

from {{ source('yellow_taxi', 'transformed_trips') }}
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_pickup_datetime >= '2026-01-01'
  AND tpep_pickup_datetime < '2026-05-01'
  AND tpep_dropoff_datetime >= '2026-01-01'
  AND tpep_dropoff_datetime < '2026-05-01'
  
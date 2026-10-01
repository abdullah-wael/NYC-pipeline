with dedub as (select
    vendorid,
    {{ dbt_utils.generate_surrogate_key([
    'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
    'pulocationid', 'dolocationid', 'total_amount'
]) }} as trip_key,
    to_number(to_char(tpep_pickup_datetime, 'YYYYMMDDHH24MI')) as pickup_datetime_key, 
    to_number(to_char(tpep_dropoff_datetime, 'YYYYMMDDHH24MI')) as dropoff_datetime_key,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    {{ dbt_utils.generate_surrogate_key(['pulocationid']) }} as pickup_location_key,
    {{ dbt_utils.generate_surrogate_key(['dolocationid']) }} as dropoff_location_key,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount_money
from {{ ref( 'transformed_trips') }}

)
select * from dedub
qualify row_number()over 
(partition by trip_key order by pickup_datetime_key) = 1
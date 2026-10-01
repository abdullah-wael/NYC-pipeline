with dates as (
    {{ dbt_utils.date_spine(
        datepart="minute",
        start_date="cast('2026-01-01 00:00:00' as timestamp)",
        end_date="cast('2026-05-01 00:00:00' as timestamp)"
    ) }}
)

select
    to_number(to_char(date_minute, 'YYYYMMDDHH24MI')) as datetime_key,
    date_minute                                       as full_timestamp,
    date_minute::date                                 as full_date,
    year(date_minute)                                 as year,
    quarter(date_minute)                              as quarter,
    month(date_minute)                                as month,
    monthname(date_minute)                            as month_name,
    day(date_minute)                                  as day_of_month,
    hour(date_minute)                                 as hour_24,
    minute(date_minute)                               as minute,
    dayofweek(date_minute)                            as day_of_week,
    dayname(date_minute)                              as day_name,
    weekofyear(date_minute)                           as week_of_year
from dates
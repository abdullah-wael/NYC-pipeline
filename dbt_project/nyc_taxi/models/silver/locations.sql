select
     locationid,
     case
          when borough = 'UNKNOWN' then 'N/A'
          else borough
     end as borough,
     case
          when zone = 'UNKNOWN' then 'N/A'
          else zone
     end as zone,
     case
          when service_zone = 'UNKNOWN' then 'N/A'
          else service_zone
     end as service_zone
from {{ source('yellow_taxi', 'locations') }}
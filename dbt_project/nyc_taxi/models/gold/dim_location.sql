select locationid ,
borough,
zone,
service_zone,
{{dbt_utils.generate_surrogate_key(['locationid'])}} as location_key
from {{ref('locations')}}
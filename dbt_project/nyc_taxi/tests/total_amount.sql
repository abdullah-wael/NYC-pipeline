select *
from {{ref('transformed_trips')}}
where total_amount_money < 0
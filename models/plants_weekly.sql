select
    rlf."Plant" plant
    , date_trunc('week', rlf."Date") date_period
    , count(*) total_count
from
    {{ ref ('recipe_log_flattened') }} as rlf
group by
    plant
    , date_period
order by
    date_period desc
    , total_count desc
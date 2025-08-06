select
    elf."Exercise" as exercise
    , date_trunc('week', elf."Date") date_period
    , count(distinct elf."Date") total_count
from
    {{ ref('exercise_log_flattened')}} as elf
group by
    exercise
    , date_period
order by
    date_period desc
    , total_count desc
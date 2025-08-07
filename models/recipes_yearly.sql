select
    rl."Dish" plant
    , date_trunc('year', rl."Date") date_period
    , count(*) total_count
from
    public.recipe_log as rl
group by
    plant
    , date_period
order by
    date_period desc
    , total_count desc
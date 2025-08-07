select
    rl."Dish" dish
    , date_trunc('quarter', rl."Date") date_period
    , count(*) total_count
from
    public.recipe_log as rl
group by
    dish
    , date_period
order by
    date_period desc
    , total_count desc
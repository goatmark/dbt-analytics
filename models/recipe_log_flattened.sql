select
  rl."Dish",
  trim(plant) as "Plant",
  rl."Date"
from
  public.recipe_log rl,
  unnest(string_to_array(rl."Plants", ',')) as plant
order by
  rl."Date" desc nulls last
select
  el."Date"
  , el."Exercise Label" as "Exercise"
  , TRIM(target_area) as "Target Area"
from
  public.exercise_log as el,
  UNNEST(string_to_array(el."Target Areas", ',')) as target_area
where
  true
  and el."Type" != 'Cardio'
order by
  el."Date" desc nulls last
  , "Exercise"
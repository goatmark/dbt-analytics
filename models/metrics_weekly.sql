{{ config(materialized='table') }}

with date_vector as (
    select
        week_start::date AS week_start,
        (week_start + interval '6 days')::date AS week_end,
        EXTRACT(week FROM week_start)::int AS week
    from
        generate_series('2022-01-03'::date, current_date + interval '30 days', interval '1 week') AS week_start
), 

exercise_metrics as (
    select
        date_trunc('week', el."Date") exercise_week
        , count(distinct el."Date") total_workouts
        , count(case when el."Exercise Label" = 'Treadmill' then 1 end) total_runs
        , sum(case when el."Exercise Label" = 'Treadmill' then el."Distance (mi)" end) total_miles
        , sum(case when el."Exercise Label" = 'Treadmill' then el."Calories" end) total_calories
        , sum(case when el."Exercise Label" = 'Treadmill' then el."Duration (min)" end) total_minutes
        , sum(case when el."Type" in ('Weights', 'Calisthenics') then el."Reps" end) total_reps
        , sum(case when el."Type" in ('Weights', 'Calisthenics') then el."Sets" end) total_sets
    from
        public.exercise_log as el
    group by
        exercise_week
)

, weight_metrics as (
    select
        date_trunc('week', w."Measurement Date") weight_week
        , count(*) total_weight_measurements
        , avg(w."Weight") average_weight
    from
        public.weights as w
    group by
        weight_week
)

, recipe_metrics as (
    with dish_flags as (
        select
            rl."Date"::date as recipe_date,
            rl."Dish",
            case
                when rl."Date" = first_value(rl."Date") over (partition by rl."Dish" order by rl."Date")
                then 'New'
                else 'Repeat'
            end as dish_type
        from public.recipe_log rl
    )
    select
        date_trunc('week', recipe_date) as recipe_week,
        count(*) as total_dishes,
        count(*) filter (where dish_type = 'New') as total_new_dishes,
        count(*) filter (where dish_type = 'Repeat') as total_repeat_dishes,
        sum(rl."Cost") as total_cost
    from
        dish_flags df
    join public.recipe_log rl on
        rl."Date" = df.recipe_date and rl."Dish" = df."Dish"
    group by
        recipe_week
)

, recipe_flattened_metrics as (
    select
        date_trunc('week', rlf."Date") recipe_week
        , count(distinct rlf."Plant") total_unique_plants
    from
        public.recipe_log_flattened as rlf
    group by
        recipe_week
)

, shopping_metrics as (
    select
        date_trunc('week', sl."Date") shopping_week
        , count(distinct sl."Ingredient") unique_ingredients_purchased
        , sum(sl."Quantity") total_ingredients_purchased
        , sum(sl."Price") total_ingredients_spend
    from
        public.shopping_log as sl
    group by
        shopping_week
)

select
    dv.week_start
    , dv.week_end
    , dv.week

    -- Execise Metrics
    , em.total_workouts
    , em.total_runs
    , em.total_miles
    , em.total_calories
    , em.total_minutes
    , em.total_reps
    , em.total_sets
    
    -- Weight Metrics
    , wm.total_weight_measurements
    , wm.average_weight

    -- Recipe Metrics
    , rm.total_dishes
    , rm.total_new_dishes
    , rm.total_repeat_dishes
    , rm.total_cost

    -- Recipe Metrics (flattened)
    , rfm.total_unique_plants

    -- Shopping Log Metrics
    , sm.unique_ingredients_purchased
    , sm.total_ingredients_purchased
    , sm.total_ingredients_spend
from
    date_vector as dv
left join exercise_metrics em on
    dv.week_start = em.exercise_week
left join weight_metrics wm on
    dv.week_start = wm.weight_week
left join recipe_metrics rm on
    dv.week_start = rm.recipe_week
left join recipe_flattened_metrics rfm on
    dv.week_start = rfm.recipe_week
left join shopping_metrics sm on
    dv.week_start = sm.shopping_week
where
    week_start <= current_date
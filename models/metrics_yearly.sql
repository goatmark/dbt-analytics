{{ config(materialized='table') }}

with date_vector as (
    select
        year_start::date as year_start,
        (year_start + interval '1 year - 1 day')::date as year_end,
        extract(year from year_start)::int as year
    from
        generate_series('2022-01-01'::date, current_date + interval '1 year', interval '1 year') as year_start
),

exercise_metrics as (
    select
        date_trunc('year', el."Date") exercise_year,
        count(distinct el."Date") total_workouts,
        count(case when el."Exercise Label" = 'Treadmill' then 1 end) total_runs,
        sum(case when el."Exercise Label" = 'Treadmill' then el."Distance (mi)" end) total_miles,
        sum(case when el."Exercise Label" = 'Treadmill' then el."Calories" end) total_calories,
        sum(case when el."Exercise Label" = 'Treadmill' then el."Duration (min)" end) total_minutes,
        sum(case when el."Type" in ('Weights', 'Calisthenics') then el."Reps" end) total_reps,
        sum(case when el."Type" in ('Weights', 'Calisthenics') then el."Sets" end) total_sets
    from
        public.exercise_log as el
    group by
        exercise_year
),

weight_metrics as (
    select
        date_trunc('year', w."Measurement Date") weight_year,
        count(*) total_weight_measurements,
        avg(w."Weight") average_weight
    from
        public.weights as w
    group by
        weight_year
),

recipe_metrics as (
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
        date_trunc('year', recipe_date) as recipe_year,
        count(*) as total_dishes,
        count(*) filter (where dish_type = 'New') as total_new_dishes,
        count(*) filter (where dish_type = 'Repeat') as total_repeat_dishes,
        sum(rl."Cost") as total_cost
    from
        dish_flags df
    join public.recipe_log rl on
        rl."Date" = df.recipe_date and rl."Dish" = df."Dish"
    group by
        recipe_year
),

recipe_flattened_metrics as (
    select
        date_trunc('year', rlf."Date") recipe_year,
        count(distinct rlf."Plant") total_unique_plants
    from
         {{ ref('recipe_log_flattened') }} as rlf
    group by
        recipe_year
),

shopping_metrics as (
    select
        date_trunc('year', sl."Date") shopping_year,
        count(distinct sl."Ingredient") unique_ingredients_purchased,
        sum(sl."Quantity") total_ingredients_purchased,
        sum(sl."Price") total_ingredients_spend
    from
        public.shopping_log as sl
    group by
        shopping_year
)

select
    dv.year_start,
    dv.year_end,
    dv.year,

    -- Exercise Metrics
    em.total_workouts,
    em.total_runs,
    em.total_miles::float8,
    em.total_calories::float8,
    em.total_minutes::float8,
    em.total_reps::float8,
    em.total_sets::float8,
    
    -- Weight Metrics
    wm.total_weight_measurements,
    wm.average_weight,

    -- Recipe Metrics
    rm.total_dishes,
    rm.total_new_dishes,
    rm.total_repeat_dishes,
    rm.total_cost,

    -- Recipe Metrics (flattened)
    rfm.total_unique_plants,

    -- Shopping Log Metrics
    sm.unique_ingredients_purchased,
    sm.total_ingredients_purchased,
    sm.total_ingredients_spend

from
    date_vector as dv
left join exercise_metrics em on
    dv.year_start = em.exercise_year
left join weight_metrics wm on
    dv.year_start = wm.weight_year
left join recipe_metrics rm on
    dv.year_start = rm.recipe_year
left join recipe_flattened_metrics rfm on
    dv.year_start = rfm.recipe_year
left join shopping_metrics sm on
    dv.year_start = sm.shopping_year
where
    dv.year_start <= current_date
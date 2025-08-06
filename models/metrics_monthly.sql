{{ config(materialized='table') }}

with date_vector as (
    select
        month_start::date as month_start,
        (month_start + interval '1 month - 1 day')::date as month_end,
        extract(month from month_start)::int as month
    from
        generate_series('2022-01-01'::date, current_date + interval '2 months', interval '1 month') as month_start
), 

exercise_metrics as (
    select
        date_trunc('month', el."Date") exercise_month,
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
        exercise_month
),

weight_metrics as (
    select
        date_trunc('month', w."Measurement Date") weight_month,
        count(*) total_weight_measurements,
        avg(w."Weight") average_weight
    from
        public.weights as w
    group by
        weight_month
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
        date_trunc('month', recipe_date) as recipe_month,
        count(*) as total_dishes,
        count(*) filter (where dish_type = 'New') as total_new_dishes,
        count(*) filter (where dish_type = 'Repeat') as total_repeat_dishes,
        sum(rl."Cost") as total_cost
    from
        dish_flags df
    join public.recipe_log rl on
        rl."Date" = df.recipe_date and rl."Dish" = df."Dish"
    group by
        recipe_month
),

recipe_flattened_metrics as (
    select
        date_trunc('month', rlf."Date") recipe_month,
        count(distinct rlf."Plant") total_unique_plants
    from
         {{ ref('recipe_log_flattened') }} as rlf
    group by
        recipe_month
),

shopping_metrics as (
    select
        date_trunc('month', sl."Date") shopping_month,
        count(distinct sl."Ingredient") unique_ingredients_purchased,
        sum(sl."Quantity") total_ingredients_purchased,
        sum(sl."Price") total_ingredients_spend
    from
        public.shopping_log as sl
    group by
        shopping_month
)

select
    dv.month_start,
    dv.month_end,
    dv.month,

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
    dv.month_start = em.exercise_month
left join weight_metrics wm on
    dv.month_start = wm.weight_month
left join recipe_metrics rm on
    dv.month_start = rm.recipe_month
left join recipe_flattened_metrics rfm on
    dv.month_start = rfm.recipe_month
left join shopping_metrics sm on
    dv.month_start = sm.shopping_month
where
    dv.month_start <= current_date

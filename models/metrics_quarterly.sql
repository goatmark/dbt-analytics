{{ config(materialized='table') }}

with date_vector as (
    select
        quarter_start::date as quarter_start,
        (quarter_start + interval '3 months - 1 day')::date as quarter_end,
        extract(quarter from quarter_start)::int as quarter
    from
        generate_series('2022-01-01'::date, current_date + interval '6 months', interval '3 months') as quarter_start
),

exercise_metrics as (
    select
        date_trunc('quarter', el."Date") exercise_quarter,
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
        exercise_quarter
),

weight_metrics as (
    select
        date_trunc('quarter', w."Measurement Date") weight_quarter,
        count(*) total_weight_measurements,
        avg(w."Weight") average_weight
    from
        public.weights as w
    group by
        weight_quarter
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
        date_trunc('quarter', recipe_date) as recipe_quarter,
        count(*) as total_dishes,
        count(*) filter (where dish_type = 'New') as total_new_dishes,
        count(*) filter (where dish_type = 'Repeat') as total_repeat_dishes,
        sum(rl."Cost") as total_cost
    from
        dish_flags df
    join public.recipe_log rl on
        rl."Date" = df.recipe_date and rl."Dish" = df."Dish"
    group by
        recipe_quarter
),

recipe_flattened_metrics as (
    select
        date_trunc('quarter', rlf."Date") recipe_quarter,
        count(distinct rlf."Plant") total_unique_plants
    from
        public.recipe_log_flattened as rlf
    group by
        recipe_quarter
),

shopping_metrics as (
    select
        date_trunc('quarter', sl."Date") shopping_quarter,
        count(distinct sl."Ingredient") unique_ingredients_purchased,
        sum(sl."Quantity") total_ingredients_purchased,
        sum(sl."Price") total_ingredients_spend
    from
        public.shopping_log as sl
    group by
        shopping_quarter
)

select
    dv.quarter_start,
    dv.quarter_end,
    dv.quarter,

    -- Exercise Metrics
    em.total_workouts,
    em.total_runs,
    em.total_miles,
    em.total_calories,
    em.total_minutes,
    em.total_reps,
    em.total_sets,
    
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
    dv.quarter_start = em.exercise_quarter
left join weight_metrics wm on
    dv.quarter_start = wm.weight_quarter
left join recipe_metrics rm on
    dv.quarter_start = rm.recipe_quarter
left join recipe_flattened_metrics rfm on
    dv.quarter_start = rfm.recipe_quarter
left join shopping_metrics sm on
    dv.quarter_start = sm.shopping_quarter
where
    dv.quarter_start <= current_date
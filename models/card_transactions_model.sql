select
    ct.key
    , ct.date
    , ct.amount
    , ct.card_last4
    , case
        when ct.type is not null then ct.type
        when ct.card_last4 not in (3221, 4245, 5083, 6823) then 'Payment'
        when ct.description ilike '%Online Transfer%' then 'Payment'
        when ct.description ilike '%Edward Jones%' then 'Payment'
        when ct.description ilike '%JPMorgan Chase%' then 'Payment'
        when ct.description ilike '%Fedwire%' then 'Payment'
        when ct.description ilike '%Automatic Payment%' then 'Payment'
        else 'Sale'
      end clean_type
    , ct.description raw_description
    , ct.category raw_category
    , ct.type raw_type
from
    public.card_transactions ct
where
    1=1
order by
    amount desc
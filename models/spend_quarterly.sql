select
    date_trunc('quarter', ct.date) date_period
    , ct.merchant_name
    , ct.category
    , ct.subcategory
    , ct.billing_model
    , ct.spend_nature
    , ct.discretion
    , ct.gl_code
    , ct.account_name
    , sum(amount) total_spend
from
    {{ (ref('classified_card_transactions'))}} as ct
where
    1=1
group by
    1
    , 2
    , 3
    , 4
    , 5
    , 6
    , 7
    , 8
    , 9
order by
    1 desc
    , total_spend asc
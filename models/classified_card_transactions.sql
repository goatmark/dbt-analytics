-- models/classified_card_transactions.sql
with base as (
  select *
  from public.card_transactions ct
  where (
    case
      when type is not null then type
      when card_last4 not in (3221,4245,5083,6823) then 'Payment'
      when description ilike '%Online Transfer%'   then 'Payment'
      when description ilike '%Edward Jones%'      then 'Payment'
      when description ilike '%JPMorgan Chase%'    then 'Payment'
      when description ilike '%Fedwire%'           then 'Payment'
      when description ilike '%Automatic Payment%' then 'Payment'
      else 'Sale'
    end
  ) = 'Sale'
),
dict as (
  select *
  from {{ ref('merchant_dictionary_full') }}
)
select
  b.*,
  d.merchant_name  as merchant,
  d.category,
  d.subcategory,
  d.billing_model,
  d.spend_nature,
  d.discretion,
  d.gl_code,
  d.account_name
from base b
left join dict d
  on b.description ~* d.pattern_regex
order by
    amount asc
-- models/classified_card_transactions.sql
with base as (
  select 
    ct.date
    , ct.description
    , ct.category category_old
    , ct.type
    , ct.amount
    , ct.card_last4
    , ct.key
  from public.card_transactions ct
  where (
    case
      when ct.type is not null then type
      when ct.card_last4 not in (3221,4245,5083,6823) then 'Payment'
      when ct.description ilike '%Online Transfer%'   then 'Payment'
      when ct.description ilike '%Edward Jones%'      then 'Payment'
      when ct.description ilike '%JPMorgan Chase%'    then 'Payment'
      when ct.description ilike '%Fedwire%'           then 'Payment'
      when ct.description ilike '%Automatic Payment%' then 'Payment'
      else 'Sale'
    end
  ) = 'Sale'
),

-- normalize description for matching
norm as (
  with raw as (
    select
      b.*,
      -- base HTML/apostrophe cleanup + collapse whitespace
      trim(
        regexp_replace(
          replace(replace(replace(b.description, '&amp;', '&'), '’', ''''), '`', ''''),
          '\s+',
          ' ',
          'g'
        )
      ) as desc_base
    from base b
  )
  select
    r.*,
    -- keep hyphens (default) -> this mirrors most patterns you already have
    r.desc_base                                   as desc_keep,
    -- remove hyphens/slashes as an alternate
    regexp_replace(r.desc_base, '\s*[-/]\s*', ' ', 'g')              as desc_nohyphen,
    -- strip aggregator prefixes (Square/Stripe/Eventbrite/PayPal-ish)
    regexp_replace(r.desc_base, '(?:SPO|SQ|EB|PY)\\s*\\*?\\s*', '', 'i') as desc_noagg,
    -- combo: no agg + no hyphen
    regexp_replace(
      regexp_replace(r.desc_base, '(?:SPO|SQ|EB|PY)\\s*\\*?\\s*', '', 'i'),
      '\s*[-/]\s*',
      ' ',
      'g'
    )                                                               as desc_noagg_nohyphen,
    -- strip a trailing ",ST" (e.g., "CHICAGO ,IL")
    regexp_replace(r.desc_base, '\s*,\s*[A-Z]{2}\s*$', '', 'i')      as desc_tail,
    -- simple canonical aliases to catch truncated airline stems, kept minimal and query-side
    case
      when r.desc_base ~* '^BRITISH A\b'   then regexp_replace(r.desc_base, '^BRITISH A\b',   'BRITISH AIRWAYS ', 1, 1, 'i')
      when r.desc_base ~* '^AMERICAN AI\b' then regexp_replace(r.desc_base, '^AMERICAN AI\b', 'AMERICAN AIRLINES ', 1, 1, 'i')
      else null
    end                                                             as desc_alias,
    -- preserve the legacy name so downstream columns don't break
    r.desc_base                                                     as desc_norm
  from raw r
),

-- best regex match (priority, then longest pattern) across ALL normalized variants
match as (
  select
    n.*,
    mr.merchant_key,
    mr.pattern_regex
  from norm n
  left join lateral (
    select r.merchant_key, r.pattern_regex, coalesce(r.priority, 0) as prio
    from {{ ref('merchant_regex') }} r
    where
         n.desc_keep            ~* r.pattern_regex
      or n.desc_nohyphen        ~* r.pattern_regex
      or n.desc_noagg           ~* r.pattern_regex
      or n.desc_noagg_nohyphen  ~* r.pattern_regex
      or n.desc_tail            ~* r.pattern_regex
      or (n.desc_alias is not null and n.desc_alias ~* r.pattern_regex)
    order by prio desc, length(r.pattern_regex) desc
    limit 1
  ) mr on true
), 

merchant_enriched as (
  select
    m.*,
    mm.merchant_name
  from match m
  left join {{ ref('merchants') }} mm
    on mm.merchant_key = m.merchant_key
),

account_keyed as (
  select
    me.*,
    map.account_id
  from merchant_enriched me
  left join {{ ref('merchant_account_map') }} map
    on map.merchant_key = me.merchant_key
),

account_enriched as (
  select
    ak.*,
    a.category,
    a.subcategory,
    a.billing_model,
    a.spend_nature,
    a.discretion,
    a.gl_code,
    a.account_name
  from account_keyed ak
  left join {{ ref('accounts_leaf') }} a
    on a.account_id = ak.account_id
)

select
    ae.date
    , ae.amount
    , ae.card_last4
    , ae.description
    , coalesce(ae.merchant_key, 'Unknown') merchant_key
    , coalesce(ae.merchant_name, 'Unknown') merchant_name
    , coalesce(ae.category, 'Unknown') category
    , coalesce(ae.subcategory, 'Unknown') subcategory
    , coalesce(ae.billing_model, 'Unknown') billing_model
    , coalesce(ae.spend_nature, 'Unknown') spend_nature
    , coalesce(ae.discretion, 'Unknown') discretion
    , coalesce(ae.gl_code, null) gl_code
    , coalesce(ae.account_name, 'Unknown') account_name
from
    account_enriched ae
where
    1=1
    and ae.type = 'Sale'
order by
    ae.date desc
    , ae.amount asc
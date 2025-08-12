-- models/classified_card_transactions.sql
-- END-STATE model: robust TST*/Square/Eventbrite/PayPal aggregator handling
-- + automatic name fallback to merchants seed (no manual regex needed for every venue).

with base as (
  select 
    ct.date
    , ct.description
    , ct.category as category_old
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
      -- clean HTML/apostrophes + collapse whitespace
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
    -- 1) keep punctuation (default view)
    r.desc_base                                   as desc_keep,
    -- 2) remove hyphens/slashes
    regexp_replace(r.desc_base, '\s*[-/]\s*', ' ', 'g')              as desc_nohyphen,
    -- 3) strip aggregator prefixes (Square/Stripe/Eventbrite/PayPal/"TST*"), allow arbitrary punctuation after tag
    regexp_replace(r.desc_base, '^(?:SPO|SQ|EB|PY|TST)\s*[^A-Za-z0-9]?\s*', '', 'i') as desc_noagg,
    -- 4) combo: no agg + no hyphen
    regexp_replace(
      regexp_replace(r.desc_base, '^(?:SPO|SQ|EB|PY|TST)\s*[^A-Za-z0-9]?\s*', '', 'i'),
      '\s*[-/]\s*',
      ' ',
      'g'
    ) as desc_noagg_nohyphen,
    -- 5) strip a trailing ", ST"
    regexp_replace(r.desc_base, '\s*,\s*[A-Z]{2}\s*$', '', 'i')      as desc_tail,
    -- 6) airline alias expansion (kept from prior logic)
    case
      when r.desc_base ~* '^BRITISH A\b'   then regexp_replace(r.desc_base, '^BRITISH A\b',   'BRITISH AIRWAYS ', 1, 1, 'i')
      when r.desc_base ~* '^AMERICAN AI\b' then regexp_replace(r.desc_base, '^AMERICAN AI\b', 'AMERICAN AIRLINES ', 1, 1, 'i')
      else null
    end                                                             as desc_alias,
    -- 7) alpha/num only, for fuzzy-ish contains checks
    lower(regexp_replace(r.desc_base, '[^A-Za-z0-9]+', ' ', 'g'))        as desc_alpha,
    lower(regexp_replace(regexp_replace(r.desc_base, '^(?:SPO|SQ|EB|PY|TST)\s*[^A-Za-z0-9]?\s*', '', 'i'),
                         '[^A-Za-z0-9]+', ' ', 'g'))                      as desc_alpha_noagg,
    r.desc_base                                                     as desc_norm
  from raw r
),

-- primary regex match (priority, then longest pattern). Demote misc buckets heavily.
match as (
  select
    n.*,
    mr.merchant_key          as rx_merchant_key,
    mr.pattern_regex         as rx_pattern,
    mr.prio                  as rx_priority
  from norm n
  left join lateral (
    select
      r.merchant_key,
      r.pattern_regex,
      (
        coalesce(r.priority, 0)
        + case
            when r.merchant_key in ('restaurants_misc', 'unknown', 'misc') then -100000
            else 0
          end
      ) as prio
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

-- name-fallback: if regex failed or landed on Restaurants (misc), match by merchant name contained in the normalized description.
-- We search across the merchants seed, pick the *longest* name hit to avoid generic "Bar", "Cafe", etc.
name_fallback as (
  select
    m.*,
    mf.merchant_key as nf_merchant_key
  from match m
  left join lateral (
    select mm.merchant_key
    from {{ ref('merchants') }} mm
    cross join lateral (
      select
        lower(regexp_replace(mm.merchant_name, '[^A-Za-z0-9]+', ' ', 'g')) as name_alpha,
        length(mm.merchant_name) as name_len
    ) t
    where
      -- Only try when the regex didn't help, or it hit the misc bucket
      (m.rx_merchant_key is null or m.rx_merchant_key = 'restaurants_misc')
      and position(t.name_alpha in m.desc_alpha_noagg) > 0
      and t.name_len >= 4
      -- weak stoplist for hyper-generic names
      and t.name_alpha not in ('bar','cafe','market','grill','store','shop')
    order by t.name_len desc -- prefer the longest specific name
    limit 1
  ) mf on true
),

-- coalesce regex → fallback
merchant_resolved as (
  select
    nf.*,
    coalesce(nf.rx_merchant_key, nf.nf_merchant_key) as merchant_key
  from name_fallback nf
),

merchant_enriched as (
  select
    m.*,
    mm.merchant_name
  from merchant_resolved m
  left join {{ ref('merchants') }} mm
    on mm.merchant_key = m.merchant_key
),

-- mapping: keep your existing seed; this deterministically picks one row if multiples exist.
map_one as (
  select merchant_key, account_id
  from (
    select
      mam.merchant_key,
      mam.account_id,
      row_number() over (
        partition by mam.merchant_key
        order by
          coalesce(mam.is_default, true) desc,
          mam.effective_from desc nulls last,
          mam.account_id desc
      ) as rn
    from {{ ref('merchant_account_map') }} mam
  ) x
  where x.rn = 1
),

account_keyed as (
  select
    me.*,
    map.account_id
  from merchant_enriched me
  left join map_one map
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
  , coalesce(ae.merchant_key, 'Unknown')       as merchant_key
  , coalesce(ae.merchant_name, 'Unknown')      as merchant_name
  , coalesce(ae.category, 'Unknown')           as category
  , coalesce(ae.subcategory, 'Unknown')        as subcategory
  , coalesce(ae.billing_model, 'Unknown')      as billing_model
  , coalesce(ae.spend_nature, 'Unknown')       as spend_nature
  , coalesce(ae.discretion, 'Unknown')         as discretion
  , coalesce(ae.account_id, 'Unknown')         as account_id
  , coalesce(ae.gl_code, null)                 as gl_code
  , coalesce(ae.account_name, 'Unknown')       as account_name
from account_enriched ae
where ae.type = 'Sale'
order by ae.date desc, ae.amount asc

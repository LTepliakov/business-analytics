
with base as
(
select          *
                , case when split_part(bundle_name,'_',2)='mb' then 'mb' else 'package' end as bundle_type
from            {{ source('aggregate', 'fact_sms_consumption_aggregate')}}
)
select          *
                , case when bundle_type = 'mb' then units else charge end as chargeable_units
from            base
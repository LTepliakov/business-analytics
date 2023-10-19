{{ config(materialized = 'ephemeral')}}

SELECT 		distinct network_id_prefix
from 		{{ source('raw', 'pd__pd_esmes') }}
where 		description = 'local'  
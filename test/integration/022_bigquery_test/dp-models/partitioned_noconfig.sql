-- Hack to make sure our events models run first.
-- In practice, these would be source data
-- {{ ref('events_20180101') }}
-- {{ ref('events_20180102') }}
-- {{ ref('events_20180103') }}

select * from `{{ this.schema }}`.`{{ date_sharded_table('events_') }}`

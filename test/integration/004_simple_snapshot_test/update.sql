-- insert v2 of the 11 - 21 records

insert into {schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
)
select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    null as dbt_valid_to,
    updated_at as dbt_updated_at,
    RAWTOHEX(
        standard_hash(
            coalesce(cast(id || '-' || first_name || '|' || TO_CHAR(updated_at) as VARCHAR2(1000 CHAR) ), ''),
            'MD5'
        )
    ) as dbt_scd_id
from {schema}.seed
where id >= 10 and id <= 20;


insert into {schema}.snapshot_castillo_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at_1,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
)
select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    null as dbt_valid_to,
    updated_at as dbt_updated_at,
    RAWTOHEX(
        standard_hash(
            coalesce(cast(id || '-' || first_name || '|' || TO_CHAR(updated_at) as VARCHAR2(1000 CHAR) ), ''),
            'MD5'
        )
    ) as dbt_scd_id
from {schema}.seed
where id >= 10 and id <= 20 and last_name = 'Castillo';


insert into {schema}.snapshot_alvarez_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
)
select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    null as dbt_valid_to,
    updated_at as dbt_updated_at,
    RAWTOHEX(
        standard_hash(
            coalesce(cast(id || '-' || first_name || '|' || TO_CHAR(updated_at) as VARCHAR2(1000 CHAR) ), ''),
            'MD5'
        )
    ) as dbt_scd_id
from {schema}.seed
where id >= 10 and id <= 20 and last_name = 'Alvarez';


insert into {schema}.snapshot_kelly_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
)
select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    null as dbt_valid_to,
    updated_at as dbt_updated_at,
    RAWTOHEX(
        standard_hash(
            coalesce(cast(id || '-' || first_name || '|' || TO_CHAR(updated_at) as VARCHAR2(1000 CHAR) ), ''),
            'MD5'
        )
    ) as dbt_scd_id
from {schema}.seed
where id >= 10 and id <= 20 and last_name = 'Kelly';

-- insert 10 new records
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (21, 'Judy', 'Robinson', 'jrobinsonk@blogs.com', 'Female', '208.21.192.232', TO_DATE('2016-09-18 08:27:38', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (22, 'Kevin', 'Alvarez', 'kalvarezl@buzzfeed.com', 'Male', '228.106.146.9', TO_DATE('2016-07-29 03:07:37', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (23, 'Barbara', 'Carr', 'bcarrm@pen.io', 'Female', '106.165.140.17', TO_DATE('2015-09-24 13:27:23', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (24, 'William', 'Watkins', 'wwatkinsn@guardian.co.uk', 'Male', '78.155.84.6', TO_DATE('2016-03-08 19:13:08', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (25, 'Judy', 'Cooper', 'jcoopero@google.com.au', 'Female', '24.149.123.184', TO_DATE('2016-10-05 20:49:33', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (26, 'Shirley', 'Castillo', 'scastillop@samsung.com', 'Female', '129.252.181.12', TO_DATE('2016-06-20 21:12:21', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (27, 'Justin', 'Harper', 'jharperq@opera.com', 'Male', '131.172.103.218', TO_DATE('2016-05-21 22:56:46', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (28, 'Marie', 'Medina', 'mmedinar@nhs.uk', 'Female', '188.119.125.67', TO_DATE('2015-10-08 13:44:33', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (29, 'Kelly', 'Edwards', 'kedwardss@phoca.cz', 'Female', '47.121.157.66', TO_DATE('2015-09-15 06:33:37', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (30, 'Carl', 'Coleman', 'ccolemant@wikipedia.org', 'Male', '82.227.154.83', TO_DATE('2016-05-26 16:46:40', 'YYYY-MM-DD HH24:MI:SS'));


-- add these new records to the snapshot table
insert into {schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
)
select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    null as dbt_valid_to,
    updated_at as dbt_updated_at,
    RAWTOHEX(
        standard_hash(
            coalesce(cast(id || '-' || first_name || '|' || TO_CHAR(updated_at) as VARCHAR2(1000 CHAR) ), ''),
            'MD5'
        )
    ) as dbt_scd_id
from {schema}.seed
where id > 20;


-- add these new records to the snapshot table
insert into {schema}.snapshot_castillo_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at_1,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
)
select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    null as dbt_valid_to,
    updated_at as dbt_updated_at,
    RAWTOHEX(
        standard_hash(
            coalesce(cast(id || '-' || first_name || '|' || TO_CHAR(updated_at) as VARCHAR2(1000 CHAR) ), ''),
            'MD5'
        )
    ) as dbt_scd_id
from {schema}.seed
where id > 20 and last_name = 'Castillo';

insert into {schema}.snapshot_alvarez_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
)
select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    null as dbt_valid_to,
    updated_at as dbt_updated_at,
    RAWTOHEX(
        standard_hash(
            coalesce(cast(id || '-' || first_name || '|' || TO_CHAR(updated_at) as VARCHAR2(1000 CHAR) ), ''),
            'MD5'
        )
    ) as dbt_scd_id
from {schema}.seed
where id > 20 and last_name = 'Alvarez';

insert into {schema}.snapshot_kelly_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
)
select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    null as dbt_valid_to,
    updated_at as dbt_updated_at,
    RAWTOHEX(
        standard_hash(
            coalesce(cast(id || '-' || first_name || '|' || TO_CHAR(updated_at) as VARCHAR2(1000 CHAR) ), ''),
            'MD5'
        )
    ) as dbt_scd_id
from {schema}.seed
where id > 20 and last_name = 'Kelly';

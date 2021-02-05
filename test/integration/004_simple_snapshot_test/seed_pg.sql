create table {schema}.seed (
	id NUMBER,
	first_name VARCHAR2(50 CHAR),
	last_name VARCHAR2(50 CHAR),
	email VARCHAR2(50 CHAR),
	gender VARCHAR2(50 CHAR),
	ip_address VARCHAR2(20 CHAR),
	updated_at TIMESTAMP(9)
);

create table {schema}.snapshot_expected (
	id NUMBER,
	first_name VARCHAR2(50 CHAR),
	last_name VARCHAR2(50 CHAR),
	email VARCHAR2(50 CHAR),
	gender VARCHAR2(50 CHAR),
	ip_address VARCHAR2(20 CHAR),

	-- snapshotting fields
	updated_at TIMESTAMP(9),
	dbt_valid_from TIMESTAMP(9),
	dbt_valid_to   TIMESTAMP(9),
	dbt_scd_id     VARCHAR2(32 CHAR),
	dbt_updated_at TIMESTAMP(9)
);


-- seed inserts
--  use the same email for two users to verify that duplicated check_cols values
--  are handled appropriately
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', TO_DATE('2015-12-24 12:19:28', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (2, 'Arthur', 'Kelly', '(not provided)', 'Male', '62.56.24.215', TO_DATE('2015-10-28 16:22:15', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (3, 'Rachel', 'Moreno', 'rmoreno2@msu.edu', 'Female', '31.222.249.23', TO_DATE('2016-04-05 02:05:30', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (4, 'Ralph', 'Turner', 'rturner3@hp.com', 'Male', '157.83.76.114', TO_DATE('2016-08-08 00:06:51', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (5, 'Laura', 'Gonzales', 'lgonzales4@howstuffworks.com', 'Female', '30.54.105.168', TO_DATE('2016-09-01 08:25:38', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (6, 'Katherine', 'Lopez', 'klopez5@yahoo.co.jp', 'Female', '169.138.46.89', TO_DATE('2016-08-30 18:52:11', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (7, 'Jeremy', 'Hamilton', 'jhamilton6@mozilla.org', 'Male', '231.189.13.133', TO_DATE('2016-07-17 02:09:46', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (8, 'Heather', 'Rose', 'hrose7@goodreads.com', 'Female', '87.165.201.65', TO_DATE('2015-12-29 22:03:56', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (9, 'Gregory', 'Kelly', 'gkelly8@trellian.com', 'Male', '154.209.99.7', TO_DATE('2016-03-24 21:18:16', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (10, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', TO_DATE('2016-08-20 15:44:49', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (11, 'Donna', 'Welch', 'dwelcha@shutterfly.com', 'Female', '103.33.110.138', TO_DATE('2016-02-27 01:41:48', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (12, 'Russell', 'Lawrence', 'rlawrenceb@qq.com', 'Male', '189.115.73.4', TO_DATE('2016-06-11 03:07:09', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (13, 'Michelle', 'Montgomery', 'mmontgomeryc@scientificamerican.com', 'Female', '243.220.95.82', TO_DATE('2016-06-18 16:27:19', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (14, 'Walter', 'Castillo', 'wcastillod@pagesperso-orange.fr', 'Male', '71.159.238.196', TO_DATE('2016-10-06 01:55:44', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (15, 'Robin', 'Mills', 'rmillse@vkontakte.ru', 'Female', '172.190.5.50', TO_DATE('2016-10-31 11:41:21', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (16, 'Raymond', 'Holmes', 'rholmesf@usgs.gov', 'Male', '148.153.166.95', TO_DATE('2016-10-03 08:16:38', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (17, 'Gary', 'Bishop', 'gbishopg@plala.or.jp', 'Male', '161.108.182.13', TO_DATE('2016-08-29 19:35:20', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (18, 'Anna', 'Riley', 'arileyh@nasa.gov', 'Female', '253.31.108.22', TO_DATE('2015-12-11 04:34:27', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (19, 'Sarah', 'Knight', 'sknighti@foxnews.com', 'Female', '222.220.3.177', TO_DATE('2016-09-26 00:49:06', 'YYYY-MM-DD HH24:MI:SS'));
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (20, 'Phyllis', 'Fox', null, 'Female', '163.191.232.95', TO_DATE('2016-08-21 10:35:19', 'YYYY-MM-DD HH24:MI:SS'));


-- populate snapshot table
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
from {schema}.seed;



create table {schema}.snapshot_castillo_expected (
    id NUMBER,
    first_name VARCHAR2(50 CHAR),
    last_name VARCHAR2(50 CHAR),
    email VARCHAR2(50 CHAR),
    gender VARCHAR2(50 CHAR),
    ip_address VARCHAR2(20 CHAR),

    -- snapshotting fields
    updated_at_1 TIMESTAMP(9),
    dbt_valid_from TIMESTAMP(9),
    dbt_valid_to   TIMESTAMP(9),
    dbt_scd_id     VARCHAR2(32 CHAR),
    dbt_updated_at TIMESTAMP(9)
);

-- one entry
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
from {schema}.seed where last_name = 'Castillo';

create table {schema}.snapshot_alvarez_expected (
    id NUMBER,
    first_name VARCHAR2(50 CHAR),
    last_name VARCHAR2(50 CHAR),
    email VARCHAR2(50 CHAR),
    gender VARCHAR2(50 CHAR),
    ip_address VARCHAR2(20 CHAR),

    -- snapshotting fields
    updated_at TIMESTAMP(9),
    dbt_valid_from TIMESTAMP(9),
    dbt_valid_to   TIMESTAMP(9),
    dbt_scd_id     VARCHAR2(32 CHAR),
    dbt_updated_at TIMESTAMP(9)
);

-- 0 entries
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
from {schema}.seed where last_name = 'Alvarez';

create table {schema}.snapshot_kelly_expected (
    id NUMBER,
    first_name VARCHAR2(50 CHAR),
    last_name VARCHAR2(50 CHAR),
    email VARCHAR2(50 CHAR),
    gender VARCHAR2(50 CHAR),
    ip_address VARCHAR2(20 CHAR),

    -- snapshotting fields
    updated_at TIMESTAMP(9),
    dbt_valid_from TIMESTAMP(9),
    dbt_valid_to   TIMESTAMP(9),
    dbt_scd_id     VARCHAR2(32 CHAR),
    dbt_updated_at TIMESTAMP(9)
);


-- 2 entries
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
from {schema}.seed where last_name = 'Kelly';

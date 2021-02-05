
create table {schema}.on_run_hook (
    "state"            VARCHAR2(4000 CHAR), -- start|end

    "target.dbname"    VARCHAR2(4000 CHAR),
    "target.host"      VARCHAR2(4000 CHAR),
    "target.name"      VARCHAR2(4000 CHAR),
    "target.schema"    VARCHAR2(4000 CHAR),
    "target.type"      VARCHAR2(4000 CHAR),
    "target.user"      VARCHAR2(4000 CHAR),
    "target.pass"      VARCHAR2(4000 CHAR),
    "target.port"      NUMBER,
    "target.threads"   NUMBER,

    "run_started_at"   VARCHAR2(4000 CHAR),
    "invocation_id"    VARCHAR2(4000 CHAR)
);

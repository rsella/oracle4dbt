create table {schema}.super_long (
    id NUMBER,
    longstring VARCHAR2(4000 CHAR),
    updated_at TIMESTAMP
);


insert into {schema}.super_long (id, longstring, updated_at) VALUES (1, 'short', current_timestamp);
insert into {schema}.super_long (id, longstring, updated_at) VALUES (2, RPAD('a', 500, 'a'), current_timestamp);

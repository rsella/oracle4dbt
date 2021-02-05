create table {schema}.expected_dep_macro (
	foo VARCHAR2(4000 CHAR),
	bar VARCHAR2(4000 CHAR)
);

create table {schema}.expected_local_macro (
	foo2 VARCHAR2(4000 CHAR),
	bar2 VARCHAR2(4000 CHAR)
);

create table {schema}.seed (
	id NUMBER,
	updated_at timestamp
);

insert into {schema}.expected_dep_macro (foo, bar)
values ('arg1', 'arg2');

insert into {schema}.expected_local_macro (foo2, bar2)
values ('arg1', 'arg2');
insert into {schema}.expected_local_macro (foo2, bar2)
values ('arg3', 'arg4');

insert into {schema}.seed (id, updated_at)
values (1, '2017-01-01');
insert into {schema}.seed (id, updated_at)
values (2, '2017-01-02');


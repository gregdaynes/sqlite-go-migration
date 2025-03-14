CREATE TABLE IF NOT EXISTS table_one
(
    column_a integer,
    column_b integer,
    column_c text,
	column_d integer default 4
);

CREATE TABLE IF NOT EXISTS table_two
(
    column_1 integer,
    column_2 integer,
    column_3 text,
	column_4 text default "four"
);

CREATE TABLE IF NOT EXISTS table_one
(
    column_a integer
        constraint table_one_pk
            primary key,
    column_b integer,
    column_c text,
	column_d integer default 4
);

CREATE UNIQUE INDEX idx_table_one_col_3
ON table_one ("column_c");

CREATE TABLE IF NOT EXISTS table_two
(
    column_1 integer,
    column_2 integer,
    column_3 text,
    column_5 text,
	column_4 text default "four"
);

CREATE UNIQUE INDEX idx_table_two_col_1
ON table_two ("column_1");

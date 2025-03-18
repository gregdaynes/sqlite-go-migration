CREATE TABLE IF NOT EXISTS table_common
(
    column_a integer
        constraint table_one_pk
            primary key,
    column_b integer,
    column_c text,
	column_d integer default 4
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_table_common_col_3
ON table_common ("column_c");

CREATE TABLE IF NOT EXISTS table_alter
(
    column_1 integer,
    column_5 text
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_table_alter_col_1
ON table_alter ("column_1");

CREATE UNIQUE INDEX IF NOT EXISTS idx_table_alter_col_5
ON table_alter ("column_5");

CREATE TABLE IF NOT EXISTS table_new
(
    column_1 integer,
    column_2 integer
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_table_new_col_2
ON table_new ("column_2")

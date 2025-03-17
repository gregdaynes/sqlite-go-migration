SQLite3 Migration
=================

An implmentation of the the migration approach described in [Simple delarative schema migration for SQLite](https://david.rothlis.net/declarative-schema-migration-for-sqlite/)


## Notes

With an existing database containing tables, we can get the create table statements from sqlite with the following query

```sql
SELECT name, sql from sqlite_schema
where type = "table" and name != "sqlite_sequence";
```

this produces a set of rows with `name` being the table name, and `sql` the create syntax

```csv
name,sql
table_one,"CREATE TABLE table_one
(
    column_a integer,
    column_b integer,
    column_c string
)"

```

Then we can get the column info for a table with

```sql
pragma table_info(table_one);
```

which produces

```csv
cid,name,type,notnull,dflt_value,pk
0,column_a,INTEGER,0,,0
1,column_b,INTEGER,0,,0
2,column_c,string,0,,0
```

---

This seems simple enough to pull off

After we get the tables from the sqlite_schema query, we can feed that into a map and find the the changes

at this point we know which tables are not the same and can proceed to step two, where we print out the columns and detect the changes made

then we start the 12 step procedure to create the new table and copy data to the new one etc

creating the new table is easy, we already have that
we can support copying data from existing columns to their equivalent in the new table, but renaming is not viable. Maybe something with a code comment or something, but that's a later task

---

CGO seems to be fine to use for sqlite. I was a little worried about it being slow to complile and use - there are some posts about it, and that we should use the go native one, but I don't think it really matters much. Go implementation was about half the speed as c - again doesn't matter, but something to keep in mind for the future

## TODO

- [x] Get existing table schema
- [x] Get pristine schema
- [x] Diff schemas
- [x] create new tables
- [x] compare table columns to find altered tables
- [x] * disable foreign keys (can probably be across all changes)
- [x] * start transaction (do we want one for all or one for each table?)
- [x] * rename the pristine table creation statement table to some prefix/suffix - no idea string manip?
- [x] * transfer content from old to new
- [x] * drop old table
- [x] * rename new table
- [ ] * create indexes again
- [ ] * create views again
- [x] * validate foreign keys `PRAGMA foreign_key_check`
- [x] * commit transaction
- [x] * re-enable foreign keys

## References

https://david.rothlis.net/declarative-schema-migration-for-sqlite/
https://www.sqlite.org/lang_altertable.html#otheralter
https://github.com/mattn/go-sqlite3
https://pkg.go.dev/modernc.org/sqlite
https://datastation.multiprocess.io/blog/2022-05-12-sqlite-in-go-with-and-without-cgo.html

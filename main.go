package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	Connection *sql.DB
	Schema     map[string]Table
}

type Table struct {
	Name    string
	SQL     string
	Columns TableColumnMap
}

type TableColumn struct {
	Name         string
	Type         string
	NotNull      bool
	DefaultValue any
	PrimaryKey   bool
}

type TableColumnMap map[string]TableColumn

func NewDB(dsn string) (db *DB) {
	db = &DB{
		Connection: connectDB(dsn),
		Schema:     make(map[string]Table),
	}

	return db
}

func (db *DB) GetSchema() map[string]Table {
	if len(db.Schema) == 0 {
		db.Schema = make(map[string]Table)

		rows, _ := db.Query(`
			SELECT name, sql from sqlite_schema
			where type = "table" and name != "sqlite_sequence";
		`)
		defer rows.Close()

		for rows.Next() {
			var name string
			var sql string

			err := rows.Scan(&name, &sql)
			if err != nil {
				log.Fatal(err)
			}

			db.Schema[name] = Table{Name: name, SQL: sql, Columns: make(TableColumnMap)}
		}
	}

	return db.Schema
}

func (db *DB) GetColumnMap(tableName string) TableColumnMap {
	if len(db.Schema[tableName].Columns) == 0 {
		// run the query to get the column
		rows, err := db.Query(`PRAGMA table_info(` + tableName + `)`)
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var id int
			var name string
			var coltype string
			var notnull int
			var dfltValue any
			var pk int

			err = rows.Scan(&id, &name, &coltype, &notnull, &dfltValue, &pk)
			if err != nil {
				log.Fatal(err)
			}

			db.Schema[tableName].Columns[name] = TableColumn{
				Name:         name,
				Type:         coltype,
				NotNull:      notnull == 1,
				DefaultValue: dfltValue,
				PrimaryKey:   pk == 1,
			}
		}
	}

	return db.Schema[tableName].Columns
}

func (db *DB) Exec(sql string) (err error) {
	_, err = db.Connection.Exec(sql)
	if err != nil {
		log.Printf("%q: %s\n", err, sql)
	}
	return err
}

func (db *DB) Query(sql string) (rows *sql.Rows, err error) {
	rows, err = db.Connection.Query(sql)
	if err != nil {
		log.Printf("%q: %s\n", err, sql)
	}
	return rows, err
}

func (db *DB) Close() (err error) {
	err = db.Connection.Close()
	return err
}

func (db *DB) removeTables(kv map[string]Table) (err error) {
	for name := range kv {
		err := db.Exec("DROP TABLE IF EXISTS " + name)
		if err != nil {
			log.Printf("%q: %s\n", err, name)
			return err
		}
	}

	return nil
}

func (db *DB) createTables(kv map[string]Table) (err error) {
	for _, table := range kv {
		err := db.Exec(table.SQL)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	// Existing DB
	CurrentDB := NewDB("file:test.db")
	defer CurrentDB.Close()

	// Temporary In Memory DB - Based on the schema.sql file
	CleanDB := NewDB("file:test.db?mode=memory")
	defer CleanDB.Close()

	schema := ReadSchema("./schema.sql")
	if err := CleanDB.Exec(schema); err != nil {
		return
	}

	newTables, tablesToDrop := diff(CleanDB.GetSchema(), CurrentDB.GetSchema())

	err := CurrentDB.removeTables(tablesToDrop)
	if err != nil {
		log.Fatal(err)
	}

	err = CurrentDB.createTables(newTables)
	if err != nil {
		log.Fatal(err)
	}

	// 1. Disable foreign keys
	err = CurrentDB.Exec("PRAGMA foreign_keys = OFF")
	if err != nil {
		log.Fatal(err)
	}

	// 2. Start transaction
	tx, err := CurrentDB.Connection.Begin()

	// 3. Define create table statement with new name
	// 4. Create new tables
	// for each altered table, we perform the operations outlined in sqlite's documentation
	for tableName := range CurrentDB.findAlteredTables(CleanDB) {
		tableNameNew := tableName + "_new"
		schema := CurrentDB.GetSchema()
		stmt := strings.Replace(schema[tableName].SQL, tableName, tableNameNew, 1)

		err = CurrentDB.Exec(stmt)
		if err != nil {
			log.Fatal(err)
		}

		// 5. Transfer table contents to new table
		// need to get the intersection of column names of the old and new table for the insert query
		intersection := intersect(
			CleanDB.GetColumnMap(tableName),
			CurrentDB.GetColumnMap(tableName),
		)

		cols := strings.Join(intersection[:], ", ")
		query := "INSERT INTO " + tableNameNew + " (" + cols + ") SELECT " + cols + " FROM " + tableName
		err = CurrentDB.Exec(query)
		if err != nil {
			log.Fatal(err)
		}

		// 6. Drop old table
		err = CurrentDB.Exec("DROP TABLE " + tableName)
		if err != nil {
			log.Fatal(err)
		}

		// 7. Rename new table to old table
		err = CurrentDB.Exec("ALTER TABLE " + tableNameNew + " RENAME TO " + tableName)
		if err != nil {
			log.Fatal(err)
		}

		// 8. Use CREATE INDEX, CREATE TRIGGER, and CREATE VIEW to reconstruct indexes, triggers, and views associated with table X. Perhaps use the old format of the triggers, indexes, and views saved from step 3 above as a guide, making changes as appropriate for the alteration.

		// 9. If any views refer to table X in a way that is affected by the schema change, then drop those views using DROP VIEW and recreate them with whatever changes are necessary to accommodate the schema change using CREATE VIEW.
	}

	// 10. If foreign key constraints were originally enabled then run PRAGMA foreign_key_check to verify that the schema change did not break any foreign key constraints.
	err = CurrentDB.Exec("PRAGMA foreign_key_check")
	if err != nil {

		log.Fatal(err)
	}

	// 11.
	err = tx.Commit()

	// 12. Enable foreign keys again
	err = CurrentDB.Exec("PRAGMA foreign_keys = ON")

	fmt.Println("Done")
}

func connectDB(dsn string) (db *sql.DB) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func ReadSchema(f string) string {
	b, err := os.ReadFile(f)
	if err != nil {
		log.Fatal(err)
	}

	return string(b)
}

func findMissingMapEntries(a, b map[string]string) (c map[string]string) {
	c = make(map[string]string)

	for key, value := range a {
		_, exists := b[key]
		if !exists {
			c[key] = value
		}
	}

	return c
}

func (db *DB) findAlteredTables(CleanDB *DB) map[string]struct{} {
	alteredTables := make(map[string]struct{})

	// Both schemas are cached before the tables were created/dropped
	// so we can compare the columns without filtering new ones out
	for name := range db.GetSchema() {
		CleanColumns := CleanDB.GetColumnMap(name)
		CurrentColumns := db.GetColumnMap(name)

		add, remove := diff(CleanColumns, CurrentColumns)

		if len(add) > 0 || len(remove) > 0 {
			alteredTables[name] = struct{}{}
		}
	}

	return alteredTables
}

func diff[T any](a, b map[string]T) (add, remove map[string]T) {
	add = make(map[string]T)
	remove = make(map[string]T)

	for k := range a {
		_, ok := b[k]
		if !ok {
			add[k] = a[k]
		}
	}

	for k := range b {
		_, ok := a[k]
		if !ok {
			remove[k] = b[k]
		}
	}

	return add, remove
}

func intersect[T any](a, b map[string]T) []string {
	intersection := []string{}

	if len(a) > len(b) {
		a, b = b, a
	}

	for k := range a {
		_, ok := b[k]
		if ok {
			intersection = append(intersection, k)
		}
	}

	return intersection
}

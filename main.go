package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	schema := schema("./schema.sql")

	// Pristine DB
	pdb := connectDB("file:test.db?mode=memory")
	defer pdb.Close()

	_, err := pdb.Exec(schema)
	if err != nil {
		log.Printf("%q: %s\n", err, schema)
		return
	}

	pmap := mapDBSchema(pdb)

	// Existing DB
	edb := connectDB("file:test.db")
	defer edb.Close()

	emap := mapDBSchema(edb)

	// new and removed tables
	nt, rt := diff(pmap, emap)

	err = removeTables(edb, rt)
	if err != nil {
		log.Fatal(err)
	}

	err = createTables(edb, nt)
	if err != nil {
		log.Fatal(err)
	}

	tablesAltered := findAlteredTables(pdb, edb, pmap, nt)
	fmt.Println(tablesAltered)

	// for each altered table, we perform the operations outlined in sqlite's documentation

}

func mapTableCols(db *sql.DB, tableName string) (cols map[string]string) {
	cols = make(map[string]string)

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

		cols[name] = "true"
	}

	return cols
}

func removeTables(db *sql.DB, kv map[string]string) (err error) {
	for name := range kv {
		_, err := db.Exec("DROP TABLE IF EXISTS " + name)
		if err != nil {
			log.Printf("%q: %s\n", err, name)
			return err
		}
	}

	return nil
}

func createTables(db *sql.DB, kv map[string]string) (err error) {
	// create new tables
	for _, sql := range kv {
		_, err := db.Exec(sql)
		if err != nil {
			return err
		}
	}

	return nil
}

func connectDB(dsn string) (db *sql.DB) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func schema(f string) string {
	b, err := os.ReadFile(f)
	if err != nil {
		log.Fatal(err)
	}

	return string(b)
}

func mapDBSchema(db *sql.DB) (smap map[string]string) {
	smap = make(map[string]string)

	// run the sqlite_schema dump
	sqlStmt := `
	SELECT name, sql from sqlite_schema
	where type = "table" and name != "sqlite_sequence";
	`
	rows, err := db.Query(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var sql string

		err = rows.Scan(&name, &sql)
		if err != nil {
			log.Fatal(err)
		}

		smap[name] = sql
	}

	return smap
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

func findAlteredTables(pdb, edb *sql.DB, pmap, nt map[string]string) map[string]struct{} {
	at := make(map[string]struct{})

	for name := range tablesToDiffColumns(pmap, nt) {
		pcols := mapTableCols(pdb, name)
		ecols := mapTableCols(edb, name)

		add, remove := diff(pcols, ecols)

		if len(add) > 0 || len(remove) > 0 {
			at[name] = struct{}{}
		}
	}

	return at
}

func tablesToDiffColumns(currentTables, newTables map[string]string) map[string]bool {
	tablesForColumns := make(map[string]bool)
	for name := range currentTables {
		_, exists := newTables[name]
		if !exists {
			tablesForColumns[name] = true
		}
	}

	return tablesForColumns
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

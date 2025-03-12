package main

import (
	"database/sql"
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

	// compare dbs
	nt := findMissingMapEntries(pmap, emap)
	rt := findMissingMapEntries(emap, pmap)

	err = removeTables(edb, rt)
	if err != nil {
		log.Fatal(err)
	}

	err = createTables(edb, nt)
	if err != nil {
		log.Fatal(err)
	}

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

// procedure - generate the schema for a pristine/new database instance
// establish a connection to an in memory database
// read in the schema.sql file
// run the statements on the pristine database
// dump the sqlite schema for the new database tables
// store in a map with the name being the key, value being the create table statement
//
// if we have an existing database, perform the operation to get the sqlite_schema
// store that as a map
// iterate the maps to find the new tables
//
// the unique new tables can have their create table statements run

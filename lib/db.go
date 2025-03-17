package lib

import (
	"database/sql"
	"log"
)

type DB struct {
	Connection *sql.DB
	Schema     Schema
}

type Schema struct {
	Tables   map[string]Table
	Indicies map[string]Index
}

func NewDB(dsn string) (db *DB) {
	db = &DB{
		Connection: connectDB(dsn),
		Schema: Schema{
			Tables:   make(map[string]Table),
			Indicies: make(map[string]Index),
		},
	}

	return db
}

func connectDB(dsn string) (db *sql.DB) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func (db *DB) Close() (err error) {
	err = db.Connection.Close()
	return err
}

func (db *DB) GetSchema() Schema {
	if len(db.Schema.Tables) == 0 {
		rows, err := db.Connection.Query(`SELECT type, name, tbl_name, sql from sqlite_schema`)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var colType string
			var name string
			var tblName string
			var sql string

			err := rows.Scan(&colType, &name, &tblName, &sql)
			if err != nil {
				log.Fatal(err)
			}

			switch colType {
			case "table":
				db.Schema.Tables[tblName] = Table{
					Name:    name,
					SQL:     sql,
					Columns: make(map[string]TableColumn),
				}

			case "index":
				db.Schema.Indicies[tblName] = Index{Name: name, SQL: sql}
			}
		}
	}

	return db.Schema
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

func (db *DB) RemoveTables(kv map[string]Table) (err error) {
	for name := range kv {
		err := db.Exec("DROP TABLE IF EXISTS " + name)
		if err != nil {
			log.Printf("%q: %s\n", err, name)
			return err
		}
	}

	return nil
}

func (db *DB) CreateTables(kv map[string]Table) (err error) {
	for _, table := range kv {
		err := db.Exec(table.SQL)
		if err != nil {
			return err
		}
	}

	return nil
}

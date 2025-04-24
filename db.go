package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "modernc.org/sqlite"
)

type DB struct {
	Connection *sql.DB
	Schema     Schema
}

type Schema struct {
	Tables   map[string]Table
	Indicies []Index
}

// NewDB creates a new DB object
// params can be a string or a slice of strings
// if params is a string, it is treated as the DSN
// if params is a slice, the first element is treated as the DSN
// if params is a slice, the second element is treated as the schema file
func NewDB(params []string) (db *DB) {
	var dsn string

	if len(params) > 0 {
		dsn = params[0]
	}

	db = &DB{
		Connection: connectDB(dsn),
		Schema: Schema{
			Tables: make(map[string]Table),
		},
	}

	if len(params) > 1 {
		schemaFile := params[1]
		if err := db.Exec(ReadSchemaFile(schemaFile)); err != nil {
			log.Fatal(err)
		}
	}

	return db
}

func connectDB(dsn string) (db *sql.DB) {
	db, err := sql.Open("sqlite", dsn)
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
	rows, err := db.Connection.Query(`SELECT type, name, tbl_name, sql FROM sqlite_schema`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var colType string
		var name string
		var tblName string
		var createSQL string

		err := rows.Scan(&colType, &name, &tblName, &createSQL)
		if err != nil {
			log.Fatal(err)
		}

		switch colType {
		case "table":
			db.Schema.Tables[tblName] = Table{
				Name:    name,
				SQL:     createSQL,
				Columns: make(map[string]TableColumn),
			}

		case "index":
			db.Schema.Indicies = append(db.Schema.Indicies, Index{Name: name, TableName: tblName, SQL: createSQL})
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
		_, err := db.Connection.Exec("DROP TABLE " + name)
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

func (db *DB) ApplySchemaChanges(CleanDB *DB) {
	newTables, tablesToDrop := Diff(CleanDB.GetSchema().Tables, db.GetSchema().Tables)

	err := db.RemoveTables(tablesToDrop)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(db.GetSchema().Tables)

	err = db.CreateTables(newTables)
	if err != nil {
		log.Fatal(err)
	}

	// New tables get new indicies
	for tableName := range newTables {
		newIndicies := CleanDB.GetSchema().GetTableIndices(tableName)
		for _, index := range newIndicies {
			err := db.Exec(index.SQL)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func (db *DB) DisableForeignKeys() {
	err := db.Exec("PRAGMA foreign_keys = OFF")
	if err != nil {
		log.Fatal(err)
	}
}

func (db *DB) GetColumns(tableName string) TableColumns {
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

		db.Schema.Tables[tableName].Columns[name] = TableColumn{
			Name:         name,
			Type:         coltype,
			NotNull:      notnull == 1,
			DefaultValue: dfltValue,
			PrimaryKey:   pk == 1,
		}
	}

	return db.Schema.Tables[tableName].Columns
}

func (db *DB) findAlteredTables(CleanDB *DB) map[string]Table {
	alteredTables := make(map[string]Table)

	// Both schemas are cached before the tables were created/dropped
	// so we can compare the columns without filtering new ones out
	for name, table := range db.GetSchema().Tables {
		_, ok := CleanDB.GetSchema().Tables[name]
		if !ok {
			continue
		}

		CleanColumns := CleanDB.GetColumns(name)
		CurrentColumns := db.GetColumns(name)

		add, remove := Diff(CleanColumns, CurrentColumns)

		if len(add) > 0 || len(remove) > 0 {
			fmt.Println("xxx", name, len(add), len(remove))
			alteredTables[name] = table
		}
	}

	return alteredTables
}

func (schema Schema) GetTableIndices(tableName string) map[string]Index {
	tableIndicies := make(map[string]Index)

	for i := 0; i < len(schema.Indicies); i++ {
		if schema.Indicies[i].TableName == tableName {
			tableIndicies[schema.Indicies[i].Name] = schema.Indicies[i]
		}
	}

	return tableIndicies
}

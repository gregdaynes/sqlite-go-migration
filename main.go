package main

import (
	"fmt"
	"log"

	"github.com/gregdaynes/sqlite-go-migration/lib"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// Existing DB
	// CurrentDB := lib.NewDB("file:test.db")
	CurrentDB := lib.NewDB("file:target.db?mode=memory")
	schemax := lib.ReadSchemaFile("./schema2.sql")
	if err := CurrentDB.Exec(schemax); err != nil {
		log.Fatal(err)
		return
	}
	defer CurrentDB.Close()

	// Temporary In Memory DB - Based on the schema.sql file
	CleanDB := lib.NewDB("file:test.db?mode=memory")
	defer CleanDB.Close()

	schema := lib.ReadSchemaFile("./schema.sql")
	if err := CleanDB.Exec(schema); err != nil {
		log.Fatal(err)
		return
	}

	newTables, tablesToDrop := lib.Diff(CleanDB.GetSchema().Tables, CurrentDB.GetSchema().Tables)

	err := CurrentDB.RemoveTables(tablesToDrop)
	if err != nil {
		log.Fatal(err)
	}

	err = CurrentDB.CreateTables(newTables)
	if err != nil {
		log.Fatal(err)
	}

	// create indicies
	newIndicies, removedIndicies := lib.Diff(CleanDB.GetSchema().Indicies, CurrentDB.GetSchema().Indicies)

	for _, index := range newIndicies {
		err := CurrentDB.Exec(index.SQL)
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, index := range removedIndicies {
		err := CurrentDB.Exec("DROP INDEX IF EXISTS " + index.Name)
		if err != nil {
			log.Fatal(err)
		}
	}

	// // 1. Disable foreign keys
	// err = CurrentDB.Exec("PRAGMA foreign_keys = OFF")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	//
	// // 2. Start transaction
	// tx, err := CurrentDB.Connection.Begin()
	//
	// // 3. Define create table statement with new name
	// // 4. Create new tables
	// // for each altered table, we perform the operations outlined in sqlite's documentation
	// for tableName, table := range CurrentDB.findAlteredTables(CleanDB) {
	// 	tableNameNew := tableName + "_new"
	//
	// 	stmt := strings.Replace(table.SQL, tableName, tableNameNew, 1)
	// 	err = CurrentDB.Exec(stmt)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	//
	// 	// 5. Transfer table contents to new table
	// 	// need to get the intersection of column names of the old and new table for the insert query
	// 	intersection := intersect(
	// 		CleanDB.GetColumnMap(tableName),
	// 		CurrentDB.GetColumnMap(tableName),
	// 	)
	// 	fmt.Println(intersection)
	//
	// 	cols := strings.Join(intersection[:], ", ")
	// 	query := "INSERT INTO " + tableNameNew + " (" + cols + ") SELECT " + cols + " FROM " + tableName
	// 	err = CurrentDB.Exec(query)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	fmt.Println("Inserted " + tableNameNew)
	//
	// 	// 6. Drop old table
	// 	err = CurrentDB.Exec("DROP TABLE " + tableName)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	fmt.Println("Dropped " + tableName)
	//
	// 	// 7. Rename new table to old table
	// 	err = CurrentDB.Exec("ALTER TABLE " + tableNameNew + " RENAME TO " + tableName)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	fmt.Println("Renamed " + tableNameNew + " to " + tableName)
	//
	// 	fmt.Println(table.Indicies)
	//
	// 	// 8. Use CREATE INDEX, CREATE TRIGGER, and CREATE VIEW to reconstruct indexes, triggers, and views associated with table X. Perhaps use the old format of the triggers, indexes, and views saved from step 3 above as a guide, making changes as appropriate for the alteration.
	// 	for _, index := range table.Indicies {
	// 		err = CurrentDB.Exec(index.SQL)
	// 		if err != nil {
	// 			log.Fatal(err)
	// 		}
	// 		fmt.Println("Created index " + index.Name)
	// 	}
	//
	// 	// 9. If any views refer to table X in a way that is affected by the schema change, then drop those views using DROP VIEW and recreate them with whatever changes are necessary to accommodate the schema change using CREATE VIEW.
	// }
	//
	// // 10. If foreign key constraints were originally enabled then run PRAGMA foreign_key_check to verify that the schema change did not break any foreign key constraints.
	// err = CurrentDB.Exec("PRAGMA foreign_key_check")
	// if err != nil {
	//
	// 	log.Fatal(err)
	// }
	//
	// // 11.
	// err = tx.Commit()
	//
	// // 12. Enable foreign keys again
	// err = CurrentDB.Exec("PRAGMA foreign_keys = ON")

	fmt.Println("🛑")
}

//	func (db *DB) GetColumnMap(tableName string) TableColumnMap {
//		if len(db.Schema[tableName].Columns) == 0 {
//			// run the query to get the column
//			rows, err := db.Query(`PRAGMA table_info(` + tableName + `)`)
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			for rows.Next() {
//				var id int
//				var name string
//				var coltype string
//				var notnull int
//				var dfltValue any
//				var pk int
//
//				err = rows.Scan(&id, &name, &coltype, &notnull, &dfltValue, &pk)
//				if err != nil {
//					log.Fatal(err)
//				}
//
//				db.Schema[tableName].Columns[name] = TableColumn{
//					Name:         name,
//					Type:         coltype,
//					NotNull:      notnull == 1,
//					DefaultValue: dfltValue,
//					PrimaryKey:   pk == 1,
//				}
//			}
//		}
//
//		return db.Schema[tableName].Columns
//	}

//
// func findMissingMapEntries(a, b map[string]string) (c map[string]string) {
// 	c = make(map[string]string)
//
// 	for key, value := range a {
// 		_, exists := b[key]
// 		if !exists {
// 			c[key] = value
// 		}
// 	}
//
// 	return c
// }
//
// func (db *DB) findAlteredTables(CleanDB *DB) map[string]Table {
// 	alteredTables := make(map[string]Table)
//
// 	// Both schemas are cached before the tables were created/dropped
// 	// so we can compare the columns without filtering new ones out
// 	for name, table := range db.GetSchema() {
// 		CleanColumns := CleanDB.GetColumnMap(name)
// 		CurrentColumns := db.GetColumnMap(name)
//
// 		add, remove := diff(CleanColumns, CurrentColumns)
//
// 		if len(add) > 0 || len(remove) > 0 {
// 			alteredTables[name] = table
// 		}
// 	}
//
// 	return alteredTables
// }
